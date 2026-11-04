"""Exercise sender demand against fake ROS producers and Zenoh matching."""
import copy
import sys
import threading
import types
import unittest
from unittest import mock

from onboard_telemetry.sender import run


CONFIG = {'connect': ['udp/127.0.0.1:7447'], 'topics': [
    {'topic': '/robot/pose', 'type': 'test/Pose', 'key': 'robot/pose', 'max_hz': 2},
    {'topic': '/robot/path', 'type': 'test/Path', 'key': 'robot/path', 'max_hz': 1},
]}


class Message:
    def __init__(self, payload):
        self._buff = payload

    def serialize(self, *_args):
        raise AssertionError('raw AnyMsg must not be reserialized')


class FakeSubscriber:
    def __init__(self, ros, topic, callback):
        self.ros = ros
        self.topic = topic
        self.callback = callback
        self.active = True
        self.unregister_calls = 0

    def unregister(self):
        with self.ros.condition:
            self.unregister_calls += 1
            self.active = False
            self.ros.condition.notify_all()
        if self.ros.unregister_hook is not None:
            self.ros.unregister_hook()
        self.ros.unregister_finished.set()


class FakeROS(types.ModuleType):
    def __init__(self):
        super().__init__('rospy')
        self.AnyMsg = type('AnyMsg', (), {})
        self.condition = threading.Condition()
        self.subscribers = []
        self.registrations = []
        self.serializations = 0
        self.callbacks = 0
        self.ready = threading.Event()
        self.shutdown = threading.Event()
        self.shutdown_hooks = []
        self.block_registration = False
        self.registration_started = threading.Event()
        self.registration_release = threading.Event()
        self.unregister_hook = None
        self.unregister_finished = threading.Event()

    def Subscriber(self, topic, raw_type, callback, queue_size):
        if queue_size != 1 or not issubclass(raw_type, self.AnyMsg):
            raise AssertionError('sender must subscribe to raw AnyMsg with queue_size=1')
        self.registration_started.set()
        if self.block_registration and not self.registration_release.wait(3):
            raise AssertionError('test did not release registration')
        subscriber = FakeSubscriber(self, topic, callback)
        with self.condition:
            self.subscribers.append(subscriber)
            self.registrations.append((topic, threading.get_ident()))
            self.condition.notify_all()
        return subscriber

    def active(self, topic):
        with self.condition:
            return [sub for sub in self.subscribers if sub.topic == topic and sub.active]

    def wait_for(self, predicate, timeout=2):
        with self.condition:
            return self.condition.wait_for(predicate, timeout)

    def publish(self, topic, payload):
        # Model actual producer work: no local serialization or delivery is
        # required if this sender has no local subscriber for the mapping.
        with self.condition:
            active = [sub for sub in self.subscribers if sub.topic == topic and sub.active]
            if active:
                self.serializations += 1
        for sub in active:
            self.callbacks += 1
            sub.callback(Message(payload))

    def on_shutdown(self, callback):
        self.shutdown_hooks.append(callback)

    def is_shutdown(self):
        return self.shutdown.is_set()

    def loginfo(self, *_args):
        self.ready.set()

    def stop(self):
        self.shutdown.set()
        for callback in self.shutdown_hooks:
            callback()


class FakeListener:
    def __init__(self, callback):
        self.callback = callback
        self.closed = False

    def undeclare(self):
        self.closed = True


class FakePublisher:
    def __init__(self, key, initially_matched=False):
        self.key = key
        self.initially_matched = initially_matched
        self.listener = None
        self.closed = False
        self.condition = threading.Condition()
        self.payloads = []

    def declare_matching_listener(self, callback):
        self.listener = FakeListener(callback)
        if self.initially_matched:
            self.match(True)
        return self.listener

    def match(self, value):
        self.listener.callback(types.SimpleNamespace(matching=value))

    def put(self, payload):
        with self.condition:
            self.payloads.append(payload)
            self.condition.notify_all()

    def wait_payloads(self, count, timeout=2):
        with self.condition:
            return self.condition.wait_for(lambda: len(self.payloads) >= count, timeout)

    def undeclare(self):
        self.closed = True


class FakeSession:
    def __init__(self, initial_keys=()):
        self.initial_keys = set(initial_keys)
        self.publishers = {}
        self.closed = False
        self.fail_publisher_key = None

    def declare_publisher(self, key, **_kwargs):
        if key == self.fail_publisher_key:
            raise RuntimeError('publisher declaration failed')
        pub = FakePublisher(key, key in self.initial_keys)
        self.publishers[key] = pub
        return pub

    def close(self):
        self.closed = True


class Runtime:
    def __init__(self, initial_keys=()):
        self.ros = FakeROS()
        self.session = FakeSession(initial_keys)
        self.errors = []
        zenoh = types.ModuleType('zenoh')
        zenoh.Config = type('Config', (), {'insert_json5': lambda *_args: None})
        zenoh.CongestionControl = types.SimpleNamespace(DROP='drop')
        zenoh.open = lambda _config: self.session
        roslib = types.ModuleType('roslib')
        roslib.__path__ = []
        message = types.ModuleType('roslib.message')
        message.get_message_class = lambda name: type('InstalledMessage', (), {
            '_type': name, '_md5sum': 'md5', '_full_text': 'definition', '_has_header': False})
        roslib.message = message
        self.modules = mock.patch.dict(sys.modules, {'rospy': self.ros, 'roslib': roslib,
                                                     'roslib.message': message, 'zenoh': zenoh})
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        try:
            run(copy.deepcopy(CONFIG))
        except BaseException as error:
            self.errors.append(error)
            self.ros.ready.set()

    def start(self):
        self.modules.start()
        self.thread.start()
        if not self.ros.ready.wait(2):
            raise AssertionError('sender did not finish listener setup')
        if self.errors:
            raise self.errors[0]
        return self

    def stop(self):
        self.ros.registration_release.set()
        self.ros.stop()
        self.thread.join(3)
        self.modules.stop()
        if self.thread.is_alive():
            raise AssertionError('sender did not join')
        if self.errors:
            raise self.errors[0]


class ProducerDemandTests(unittest.TestCase):
    def setUp(self):
        self.runtime = Runtime().start()
        self.addCleanup(self.runtime.stop)
        self.ros = self.runtime.ros
        self.pub = self.runtime.session.publishers['robot/pose']

    def test_no_match_creates_no_source_work_and_reacquisition_rejects_old_callbacks(self):
        for _ in range(100):
            self.ros.publish('/robot/pose', b'unmatched')
        self.assertEqual(len(self.ros.subscribers), 0)
        self.assertEqual(self.ros.serializations, 0)
        self.assertEqual(self.ros.callbacks, 0)

        self.pub.match(True)
        self.assertTrue(self.ros.wait_for(lambda: len(self.ros.subscribers) == 1))
        first = self.ros.subscribers[0]
        self.assertEqual(first.topic, '/robot/pose')
        for _ in range(20):
            self.pub.match(True)
        # A different mapping provides a barrier through the same owner loop.
        path_pub = self.runtime.session.publishers['robot/path']
        path_pub.match(True)
        self.assertTrue(self.ros.wait_for(lambda: len(self.ros.subscribers) >= 2))
        self.assertEqual(len(self.ros.active('/robot/pose')), 1)
        self.assertEqual(sum(sub.topic == '/robot/pose' for sub in self.ros.subscribers), 1)
        self.ros.publish('/robot/pose', b'\x00\xffraw')
        self.assertTrue(self.pub.wait_payloads(1))
        self.assertEqual(self.pub.payloads, [b'\x00\xffraw'])

        # A throttled value is pending when the last consumer leaves.
        self.ros.publish('/robot/pose', b'pending-old-generation')
        self.pub.match(False)
        self.assertTrue(self.ros.wait_for(lambda: not first.active))
        self.assertEqual(first.unregister_calls, 1)
        for _ in range(100):
            self.ros.publish('/robot/pose', b'while-unmatched')
        self.assertEqual(self.ros.serializations, 2)
        self.pub.match(True)
        self.assertTrue(self.ros.wait_for(lambda: len(self.ros.subscribers) == 3))
        first.callback(Message(b'old-generation'))
        self.assertFalse(self.pub.wait_payloads(2, 0.65))
        self.ros.publish('/robot/pose', b'new-generation')
        self.assertTrue(self.pub.wait_payloads(2))
        self.assertEqual(self.pub.payloads, [b'\x00\xffraw', b'new-generation'])

    def test_blocked_ros_registration_does_not_block_matching_callback_or_leak_stale_binding(self):
        self.ros.block_registration = True
        self.pub.match(True)
        self.assertTrue(self.ros.registration_started.wait(2))
        callback = threading.Thread(target=lambda: (self.pub.match(False), self.pub.match(True)))
        callback.start()
        callback.join(0.5)
        self.assertFalse(callback.is_alive(), 'matching callback blocked on ROS registration')
        self.ros.registration_release.set()
        self.assertTrue(self.ros.wait_for(lambda: len(self.ros.subscribers) == 2))
        old, current = self.ros.subscribers
        self.assertFalse(old.active)
        self.assertTrue(current.active)
        old.callback(Message(b'stale'))
        self.assertFalse(self.pub.wait_payloads(1, 0.15))
        self.ros.publish('/robot/pose', b'fresh')
        self.assertTrue(self.pub.wait_payloads(1))
        self.assertEqual(self.pub.payloads, [b'fresh'])

    def test_transient_demand_is_coalesced_while_another_mapping_registers(self):
        self.ros.block_registration = True
        path_pub = self.runtime.session.publishers['robot/path']
        path_pub.match(True)
        self.assertTrue(self.ros.registration_started.wait(2))
        self.pub.match(True)
        self.pub.match(False)
        self.ros.registration_release.set()
        self.assertTrue(self.ros.wait_for(lambda: len(self.ros.subscribers) == 1))
        path = self.ros.subscribers[0]
        self.assertEqual(path.topic, '/robot/path')
        path_pub.match(False)
        self.assertTrue(self.ros.wait_for(lambda: not path.active))
        self.assertEqual([sub.topic for sub in self.ros.subscribers], ['/robot/path'])

    def test_shutdown_joins_after_registration_and_closes_all_resources(self):
        self.ros.block_registration = True
        self.pub.match(True)
        self.assertTrue(self.ros.registration_started.wait(2))
        self.ros.stop()
        self.ros.registration_release.set()
        self.runtime.thread.join(3)
        self.assertFalse(self.runtime.thread.is_alive())
        self.assertTrue(self.runtime.session.closed)
        self.assertTrue(all(pub.closed and pub.listener.closed for pub in self.runtime.session.publishers.values()))
        self.assertTrue(all(not sub.active and sub.unregister_calls == 1 for sub in self.ros.subscribers))
        count = len(self.ros.subscribers)
        self.pub.match(True)  # A late SDK callback after close cannot reattach ROS.
        self.assertEqual(len(self.ros.subscribers), count)

    def test_ros_unregister_does_not_hold_the_matching_callback_lock(self):
        self.pub.match(True)
        self.assertTrue(self.ros.wait_for(lambda: len(self.ros.subscribers) == 1))

        def callback_during_unregister():
            worker = threading.Thread(target=lambda: self.pub.match(False))
            worker.start()
            worker.join(0.5)
            if worker.is_alive():
                raise AssertionError('matching callback blocked behind ROS unregistration')

        self.ros.unregister_hook = callback_during_unregister
        self.pub.match(False)
        self.assertTrue(self.ros.unregister_finished.wait(2))
        self.assertEqual(self.runtime.errors, [])

    def test_cleanup_attempts_all_resources_when_one_listener_fails(self):
        def fail():
            raise RuntimeError('listener close failed')

        self.pub.listener.undeclare = fail
        self.ros.stop()
        self.runtime.thread.join(2)
        self.assertFalse(self.runtime.thread.is_alive())
        self.assertEqual(str(self.runtime.errors[0]), 'listener close failed')
        self.assertTrue(self.runtime.session.closed)
        self.assertTrue(all(pub.closed for pub in self.runtime.session.publishers.values()))
        self.assertTrue(self.runtime.session.publishers['robot/path'].listener.closed)
        self.runtime.errors.clear()  # The expected error has been asserted.


class InitialMatchTests(unittest.TestCase):
    def test_existing_zenoh_consumer_triggers_subscription(self):
        runtime = Runtime(initial_keys={'robot/pose'}).start()
        self.addCleanup(runtime.stop)
        self.assertTrue(runtime.ros.wait_for(lambda: len(runtime.ros.active('/robot/pose')) == 1))
        self.assertEqual(len(runtime.ros.active('/robot/path')), 0)

    def test_partial_publisher_setup_releases_previous_publisher_and_session(self):
        runtime = Runtime()
        runtime.session.fail_publisher_key = 'robot/path'
        with runtime.modules, self.assertRaisesRegex(RuntimeError, 'publisher declaration failed'):
            run(copy.deepcopy(CONFIG))
        self.assertTrue(runtime.session.closed)
        self.assertTrue(runtime.session.publishers['robot/pose'].closed)
        self.assertEqual(runtime.ros.subscribers, [])
