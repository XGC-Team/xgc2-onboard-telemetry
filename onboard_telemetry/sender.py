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
                        'pending': None, 'matched': False} for item in mappings]
        self._closed = False

    def set_matched(self, index, matched):
        with self._condition:
            slot = self._slots[index]
            slot['matched'] = matched
            if not matched:
                # Never replay samples captured for a departed receiver.
                slot['pending'] = None
            self._condition.notify()

    def is_matched(self, index):
        with self._condition:
            return not self._closed and self._slots[index]['matched']

    def offer(self, index, payload):
        with self._condition:
            if self._closed or not self._slots[index]['matched']:
                return
            slot = self._slots[index]
            was_empty = slot['pending'] is None
            slot['pending'] = payload
            if was_empty:
                self._condition.notify()

    def close(self):
        with self._condition:
            self._closed = True
            self._condition.notify_all()

    def take(self):
        with self._condition:
            while not self._closed:
                waiting = [(slot['next'], index) for index, slot in enumerate(self._slots)
                           if slot['pending'] is not None]
                if not waiting:
                    self._condition.wait()
                    continue
                deadline, index = min(waiting)
                delay = deadline - self._clock()
                if delay > 0:
                    self._condition.wait(delay)
                    continue
                payload = self._slots[index]['pending']
                self._slots[index]['pending'] = None
                return index, payload
            return None

    def sent(self, index):
        with self._condition:
            self._slots[index]['next'] = self._clock() + self._slots[index]['interval']


def zenoh_config(zenoh, endpoints):
    config = zenoh.Config()
    # Peer mode can start disconnected and reconnect in the SDK background.
    # With no listeners or scouting, it only dials the configured endpoints.
    config.insert_json5('mode', '"peer"')
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
    subscribers = []
    publishers = []
    listeners = []
    try:
        publishers = [session.declare_publisher(item['key'], congestion_control=zenoh.CongestionControl.DROP)
                      for item in config['topics']]
        for index, publisher in enumerate(publishers):
            listeners.append(publisher.declare_matching_listener(
                lambda status, i=index: scheduler.set_matched(i, status.matching)))
        rospy.on_shutdown(scheduler.close)
        for index, (item, raw_type) in enumerate(zip(config['topics'], raw_types)):
            subscribers.append(rospy.Subscriber(item['topic'], raw_type,
                lambda message, i=index: scheduler.offer(i, message._buff), queue_size=1))
        rospy.loginfo('onboard telemetry: forwarding %d configured topics', len(subscribers))
        while not rospy.is_shutdown():
            entry = scheduler.take()
            if entry is None:
                break
            index, payload = entry
            if scheduler.is_matched(index):
                publishers[index].put(payload)
                scheduler.sent(index)
    finally:
        scheduler.close()
        for subscriber in subscribers:
            subscriber.unregister()
        for listener in listeners:
            listener.undeclare()
        for publisher in publishers:
            publisher.undeclare()
        session.close()
