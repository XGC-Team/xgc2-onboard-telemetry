"""Forward configured raw ROS messages with one latest-value scheduler."""
import json
import threading
import time


class LatestScheduler:
    """One pending value per mapping; callbacks never send or queue history."""
    def __init__(self, mappings, clock=time.monotonic):
        self._condition = threading.Condition()
        self._clock = clock
        self._slots = [{'interval': 1.0 / item['max_hz'], 'next': 0.0,
                        'pending': None, 'matched': False, 'generation': 0,
                        'reconcile': False} for item in mappings]
        self._closed = False
        self._waiting_until = None

    def set_matched(self, index, matched):
        with self._condition:
            if self._closed:
                return None
            slot = self._slots[index]
            matched = bool(matched)
            if slot['matched'] == matched:
                return slot['generation']
            slot['matched'] = matched
            slot['generation'] += 1
            # A new match cannot inherit values captured by a retired binding.
            slot['pending'] = None
            slot['reconcile'] = True
            self._condition.notify()
            return slot['generation']

    def is_matched(self, index, generation):
        with self._condition:
            slot = self._slots[index]
            return not self._closed and slot['matched'] and slot['generation'] == generation

    def offer(self, index, generation, payload):
        with self._condition:
            slot = self._slots[index]
            if self._closed or not slot['matched'] or slot['generation'] != generation:
                return
            was_empty = slot['pending'] is None
            slot['pending'] = payload
            # Only the single take() owner waits here. A later deadline cannot
            # change its current wake-up; an earlier one must interrupt it.
            if was_empty and self._waiting_until is not None and slot['next'] < self._waiting_until:
                self._condition.notify()

    def close(self):
        with self._condition:
            self._closed = True
            self._condition.notify_all()

    def take(self):
        with self._condition:
            while not self._closed:
                for index, slot in enumerate(self._slots):
                    if slot['reconcile']:
                        slot['reconcile'] = False
                        return 'match', index, slot['generation'], slot['matched']
                waiting = [(slot['next'], index) for index, slot in enumerate(self._slots)
                           if slot['pending'] is not None]
                if not waiting:
                    self._waiting_until = float('inf')
                    try:
                        self._condition.wait()
                    finally:
                        self._waiting_until = None
                    continue
                deadline, index = min(waiting)
                delay = deadline - self._clock()
                if delay > 0:
                    self._waiting_until = deadline
                    try:
                        self._condition.wait(delay)
                    finally:
                        self._waiting_until = None
                    continue
                payload = self._slots[index]['pending']
                self._slots[index]['pending'] = None
                return 'sample', index, self._slots[index]['generation'], payload
            return None

    def sent(self, index):
        with self._condition:
            self._slots[index]['next'] = self._clock() + self._slots[index]['interval']


def zenoh_config(zenoh, endpoints):
    config = zenoh.Config()
    # Client mode selects the workflow-owned router and dials only the
    # explicitly configured endpoint. SDK open success is the connection fact.
    config.insert_json5('mode', '"client"')
    config.insert_json5('listen/endpoints', '[]')
    config.insert_json5('connect/endpoints', json.dumps(endpoints))
    config.insert_json5('connect/timeout_ms', '-1')
    config.insert_json5('connect/exit_on_failure', 'false')
    config.insert_json5('connect/retry', json.dumps({
        'period_init_ms': 1000, 'period_max_ms': 4000, 'period_increase_factor': 2.0}))
    config.insert_json5('open/return_conditions/connect_scouted', 'false')
    config.insert_json5('scouting/multicast/enabled', 'false')
    config.insert_json5('scouting/gossip/enabled', 'false')
    return config


def run(config):
    import rospy
    import roslib.message
    import zenoh

    # Use the generated class only for the ROS type/MD5 handshake. AnyMsg's
    # deserializer retains the original serialized payload without decoding it.
    raw_types = []
    for item in config['topics']:
        message_class = roslib.message.get_message_class(item['type'])
        if message_class is None:
            raise ValueError('ROS message type is not installed: ' + item['type'])
        raw_types.append(type('Raw_' + item['type'].replace('/', '_'), (rospy.AnyMsg,), {
            '_type': message_class._type, '_md5sum': message_class._md5sum,
            '_full_text': message_class._full_text, '_has_header': message_class._has_header,
        }))

    scheduler = LatestScheduler(config['topics'])
    session = zenoh.open(zenoh_config(zenoh, config['connect']))
    subscribers = [None] * len(config['topics'])
    publishers = []
    listeners = []
    try:
        for item in config['topics']:
            publishers.append(session.declare_publisher(item['key'], congestion_control=zenoh.CongestionControl.DROP))
        for index, publisher in enumerate(publishers):
            listeners.append(publisher.declare_matching_listener(
                lambda status, i=index: scheduler.set_matched(i, status.matching)))
        rospy.on_shutdown(scheduler.close)
        rospy.loginfo('onboard telemetry: configured %d topics', len(config['topics']))
        while not rospy.is_shutdown():
            entry = scheduler.take()
            if entry is None:
                break
            kind, index, generation, value = entry
            if kind == 'match':
                previous = subscribers[index]
                if previous is not None:
                    previous.unregister()
                    subscribers[index] = None
                if value and scheduler.is_matched(index, generation):
                    item = config['topics'][index]
                    subscriber = rospy.Subscriber(item['topic'], raw_types[index],
                        lambda message, i=index, g=generation: scheduler.offer(i, g, message._buff), queue_size=1)
                    subscribers[index] = subscriber
                    if not scheduler.is_matched(index, generation):
                        subscriber.unregister()
                        subscribers[index] = None
                continue
            if scheduler.is_matched(index, generation):
                publishers[index].put(value)
                scheduler.sent(index)
    finally:
        scheduler.close()
        cleanup_error = None
        resources = ([(item, 'unregister') for item in subscribers if item is not None]
                     + [(item, 'undeclare') for item in listeners]
                     + [(item, 'undeclare') for item in publishers] + [(session, 'close')])
        for item, method in resources:
            try:
                getattr(item, method)()
            except Exception as error:
                if cleanup_error is None:
                    cleanup_error = error
        if cleanup_error is not None:
            raise cleanup_error
