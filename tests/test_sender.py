import copy
import json
import subprocess
import sys
import threading
import time
import unittest
from unittest import mock

from onboard_telemetry.config import ConfigError, parse_config
from onboard_telemetry.sender import LatestScheduler


CONFIG = {'connect': ['tcp/127.0.0.1:7447'], 'topics': [
    {'topic': '/vehicle/pose', 'type': 'geometry_msgs/PoseStamped', 'key': 'vehicle/pose', 'max_hz': 20},
    {'topic': '/vehicle/path', 'type': 'nav_msgs/Path', 'key': 'vehicle/path', 'max_hz': 1}]}


class ConfigTests(unittest.TestCase):
    def test_explicit_mapping(self):
        self.assertEqual(parse_config(copy.deepcopy(CONFIG)), CONFIG)

    def test_cli_requires_exactly_one_config_source(self):
        for arguments in ([], ['--config', 'unused', '--config-json', json.dumps(CONFIG)]):
            result = subprocess.run([sys.executable, '-m', 'onboard_telemetry'] + arguments,
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 2, result.stderr)
        result = subprocess.run([sys.executable, '-m', 'onboard_telemetry', '--config-json', '{invalid'],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn('onboard-telemetry:', result.stderr)

    def test_process_definition_uses_direct_arguments_without_a_service(self):
        from pathlib import Path
        path = Path(__file__).resolve().parents[1] / 'process-definitions/xgc2-onboard-telemetry.json'
        document = json.loads(path.read_text())
        self.assertEqual(document['apiVersion'], 'xgc.execution.process/v1')
        definition = document['definitions'][0]
        self.assertEqual(definition['id'], 'xgc2-onboard-telemetry')
        self.assertEqual(definition['setupFiles'], ['/opt/ros/noetic/setup.bash'])
        self.assertEqual(definition['parameters']['properties']['configuration']['type'], 'json')
        self.assertEqual(definition['command']['args'], ['--config-json', '${configuration}'])
        self.assertEqual(definition['command']['executable'], '/usr/bin/onboard-telemetry')
        self.assertEqual(definition['command']['envParameter'], 'environment')
        self.assertEqual(definition['readiness']['kind'], 'none')
        self.assertNotIn('services', definition)
        self.assertEqual(definition['stop']['signal'], 'SIGINT')
        self.assertEqual(set(definition['parameters']['required']), {'configuration', 'rosMasterUri', 'rosIp'})

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
    @staticmethod
    def sample(scheduler):
        while True:
            event = scheduler.take()
            if event is None:
                return None
            if event[0] == 'sample':
                return event[1], event[3]

    def test_latest_only_and_no_periodic_replay(self):
        scheduler = LatestScheduler(CONFIG['topics'])
        generations = [scheduler.set_matched(index, True) for index in range(len(CONFIG['topics']))]
        scheduler.offer(0, generations[0], b'first')
        self.assertEqual(self.sample(scheduler), (0, b'first'))
        scheduler.sent(0)
        for value in (b'old', b'newer', b'latest'): scheduler.offer(0, generations[0], value)
        start = time.monotonic()
        self.assertEqual(self.sample(scheduler), (0, b'latest'))
        self.assertGreaterEqual(time.monotonic() - start, 0.035)
        scheduler.sent(0)
        result = []
        thread = threading.Thread(target=lambda: result.append(scheduler.take()))
        thread.start(); time.sleep(0.08)
        self.assertTrue(thread.is_alive())
        scheduler.close(); thread.join(1)
        self.assertEqual(result, [None])

    def test_unmatched_inputs_are_discarded_and_pending_is_cleared(self):
        scheduler = LatestScheduler(CONFIG['topics'])
        for _ in range(10000): scheduler.offer(0, 0, b'unmatched')
        old = scheduler.set_matched(0, True)
        scheduler.offer(0, old, b'old')
        scheduler.set_matched(0, False)
        current = scheduler.set_matched(0, True)
        scheduler.offer(0, old, b'late-old-callback')
        scheduler.offer(0, current, b'fresh')
        self.assertEqual(self.sample(scheduler), (0, b'fresh'))
        scheduler.close()

    def test_other_topic_not_blocked_by_throttled_slot(self):
        scheduler = LatestScheduler(CONFIG['topics'])
        generations = [scheduler.set_matched(index, True) for index in range(len(CONFIG['topics']))]
        scheduler.offer(1, generations[1], b'first'); self.assertEqual(self.sample(scheduler), (1, b'first')); scheduler.sent(1)
        scheduler.offer(1, generations[1], b'waiting'); scheduler.offer(0, generations[0], b'now')
        self.assertEqual(self.sample(scheduler), (0, b'now'))
        scheduler.close()

    def test_later_deadline_does_not_wake_a_waiter_for_an_earlier_one(self):
        waiting = threading.Event()
        conditions = []

        class CountingCondition(threading.Condition):
            def __init__(self):
                super().__init__()
                self.notifications = 0
                conditions.append(self)

            def wait(self, timeout=None):
                if timeout is not None:
                    waiting.set()
                return super().wait(timeout)

            def notify(self, n=1):
                self.notifications += 1
                return super().notify(n)

        with mock.patch('onboard_telemetry.sender.threading.Condition', CountingCondition):
            scheduler = LatestScheduler([{'max_hz': 0.1}, {'max_hz': 0.05}, {'max_hz': 0.2}], clock=lambda: 0.0)
        generations = [scheduler.set_matched(i, True) for i in range(3)]
        for _ in generations:
            self.assertEqual(scheduler.take()[0], 'match')
        for index, generation in enumerate(generations):
            scheduler.offer(index, generation, b'first')
            self.assertEqual(self.sample(scheduler), (index, b'first'))
            scheduler.sent(index)
        scheduler.offer(0, generations[0], b'earlier-pending')
        worker = threading.Thread(target=scheduler.take)
        worker.start()
        try:
            self.assertTrue(waiting.wait(1), 'scheduler did not wait on the first deadline')
            before = conditions[0].notifications
            scheduler.offer(1, generations[1], b'later-pending')
            self.assertEqual(conditions[0].notifications - before, 0)
            scheduler.offer(2, generations[2], b'new-earliest')
            self.assertEqual(conditions[0].notifications - before, 1)
        finally:
            scheduler.close()
            worker.join(1)
            self.assertFalse(worker.is_alive())


if __name__ == '__main__':
    unittest.main()
