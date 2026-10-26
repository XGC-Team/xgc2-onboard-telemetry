import copy
import threading
import time
import unittest

from onboard_telemetry.config import ConfigError, parse_config
from onboard_telemetry.sender import LatestScheduler


CONFIG = {'connect': ['tcp/127.0.0.1:7447'], 'topics': [
    {'topic': '/vehicle/pose', 'type': 'geometry_msgs/PoseStamped', 'key': 'vehicle/pose', 'max_hz': 20},
    {'topic': '/vehicle/path', 'type': 'nav_msgs/Path', 'key': 'vehicle/path', 'max_hz': 1}]}


class ConfigTests(unittest.TestCase):
    def test_explicit_mapping(self):
        self.assertEqual(parse_config(copy.deepcopy(CONFIG)), CONFIG)

    def test_rejects_missing_or_ambiguous_configuration(self):
        cases = []
        for change in ('empty_connect', 'relative_topic', 'wildcard_key', 'bad_type', 'nan_rate', 'zero_rate', 'duplicate_key', 'extra'):
            config = copy.deepcopy(CONFIG)
            if change == 'empty_connect': config['connect'] = []
            if change == 'relative_topic': config['topics'][0]['topic'] = 'vehicle/pose'
            if change == 'wildcard_key': config['topics'][0]['key'] = 'vehicle/**'
            if change == 'bad_type': config['topics'][0]['type'] = '*'
            if change == 'nan_rate': config['topics'][0]['max_hz'] = float('nan')
            if change == 'zero_rate': config['topics'][0]['max_hz'] = 0
            if change == 'duplicate_key': config['topics'][1]['key'] = config['topics'][0]['key']
            if change == 'extra': config['discover'] = True
            cases.append((change, config))
        for name, config in cases:
            with self.subTest(name=name), self.assertRaises(ConfigError): parse_config(config)


class SchedulerTests(unittest.TestCase):
    def test_latest_only_and_no_periodic_replay(self):
        scheduler = LatestScheduler(CONFIG['topics'])
        scheduler.offer(0, b'first')
        self.assertEqual(scheduler.take(), (0, b'first'))
        scheduler.sent(0)
        for value in (b'old', b'newer', b'latest'): scheduler.offer(0, value)
        start = time.monotonic()
        self.assertEqual(scheduler.take(), (0, b'latest'))
        self.assertGreaterEqual(time.monotonic() - start, 0.035)
        scheduler.sent(0)
        result = []
        thread = threading.Thread(target=lambda: result.append(scheduler.take()))
        thread.start(); time.sleep(0.08)
        self.assertTrue(thread.is_alive())
        scheduler.close(); thread.join(1)
        self.assertEqual(result, [None])

    def test_other_topic_not_blocked_by_throttled_slot(self):
        scheduler = LatestScheduler(CONFIG['topics'])
        scheduler.offer(1, b'first'); self.assertEqual(scheduler.take(), (1, b'first')); scheduler.sent(1)
        scheduler.offer(1, b'waiting'); scheduler.offer(0, b'now')
        self.assertEqual(scheduler.take(), (0, b'now'))
        scheduler.close()


if __name__ == '__main__':
    unittest.main()
