#!/usr/bin/env python3
"""Exercise a real Noetic master, ROS publishers, sender, and Zenoh peer."""
import io
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import xmlrpc.client

import rospy
from std_msgs.msg import Float64, String, UInt32
import zenoh
from onboard_telemetry.sender import zenoh_config


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def wait_until(condition, timeout, description):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if condition():
            return
        time.sleep(0.02)
    raise AssertionError('timed out waiting for ' + description)


def serialized(message):
    buffer = io.BytesIO()
    message.serialize(buffer)
    return buffer.getvalue()


def main():
    port, zenoh_port = free_port(), free_port()
    os.environ['ROS_MASTER_URI'] = 'http://127.0.0.1:{}'.format(port)
    os.environ['ROS_HOSTNAME'] = '127.0.0.1'
    master = subprocess.Popen(['roscore', '-p', str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    sender = None
    session = None
    with tempfile.TemporaryDirectory() as tmp:
        try:
            def master_ready():
                try:
                    return xmlrpc.client.ServerProxy(os.environ['ROS_MASTER_URI']).getUri('/test')[0] == 1
                except OSError:
                    return False
            wait_until(master_ready, 15, 'isolated roscore')
            zconfig = zenoh.Config()
            for key, value in [('mode', '"peer"'), ('listen/endpoints', json.dumps(['tcp/127.0.0.1:{}'.format(zenoh_port)])),
                               ('scouting/multicast/enabled', 'false'), ('scouting/gossip/enabled', 'false')]:
                zconfig.insert_json5(key, value)
            session = zenoh.open(zconfig)
            received = []
            lock = threading.Lock()
            def receive(sample):
                with lock: received.append((str(sample.key_expr), sample.payload.to_bytes(), time.monotonic()))
            sub = session.declare_subscriber('test/**', receive)
            config = {'connect': ['tcp/127.0.0.1:{}'.format(zenoh_port)], 'topics': [
                {'topic': '/test/count', 'type': 'std_msgs/UInt32', 'key': 'test/count', 'max_hz': 2},
                {'topic': '/test/text', 'type': 'std_msgs/String', 'key': 'test/text', 'max_hz': 10},
                {'topic': '/test/raw', 'type': 'std_msgs/Float64', 'key': 'test/raw', 'max_hz': 10},
                {'topic': '/test/wrong_type', 'type': 'std_msgs/UInt32', 'key': 'test/wrong', 'max_hz': 10}]}
            config_path = os.path.join(tmp, 'config.json')
            with open(config_path, 'w') as f: json.dump(config, f)
            sender_config = zenoh_config(zenoh, config['connect'])
            assert json.loads(sender_config.get_json('mode')) == 'peer'
            assert json.loads(sender_config.get_json('listen/endpoints')) == []
            assert json.loads(sender_config.get_json('connect/timeout_ms')) == -1
            assert json.loads(sender_config.get_json('connect/exit_on_failure')) is False
            assert json.loads(sender_config.get_json('connect/endpoints')) == config['connect']
            assert json.loads(sender_config.get_json('scouting/multicast/enabled')) is False
            assert json.loads(sender_config.get_json('scouting/gossip/enabled')) is False
            rospy.init_node('telemetry_test', anonymous=True, disable_signals=True)
            count = rospy.Publisher('/test/count', UInt32, queue_size=1)
            text = rospy.Publisher('/test/text', String, queue_size=1)
            class RawFloat(rospy.AnyMsg):
                _type = Float64._type
                _md5sum = Float64._md5sum
                _full_text = Float64._full_text
                _has_header = Float64._has_header
            raw = rospy.Publisher('/test/raw', RawFloat, queue_size=1)
            ignored = rospy.Publisher('/test/not_configured', String, queue_size=1)
            wrong = rospy.Publisher('/test/wrong_type', String, queue_size=1)
            definition_path = os.environ.get('TELEMETRY_PROCESS_DEFINITION')
            sender_env = os.environ.copy()
            if definition_path:
                with open(definition_path) as f:
                    definition = json.load(f)['definitions'][0]
                params = {'configuration': config, 'rosMasterUri': os.environ['ROS_MASTER_URI'], 'rosIp': '127.0.0.1'}
                renderer = os.environ.get('TELEMETRY_RENDERER')
                encoded = json.dumps(params['configuration'], separators=(',', ':'))
                if renderer:
                    encoded = subprocess.run([renderer], input=encoded, text=True, stdout=subprocess.PIPE, check=True).stdout
                    assert json.loads(encoded) == params['configuration']
                command = [definition['command']['executable']] + [encoded if arg == '${configuration}' else arg for arg in definition['command']['args']]
                for key, value in definition['command']['env'].items():
                    sender_env[key] = params[value[2:-1]] if value.startswith('${') else value
            else:
                executable = os.environ.get('TELEMETRY_BIN')
                command = ([executable] if executable else [sys.executable, '-m', 'onboard_telemetry']) + ['--config', config_path]
            sender = subprocess.Popen(command, env=sender_env,
                                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            wait_until(lambda: sender.poll() is not None or (count.get_num_connections() == 1 and text.get_num_connections() == 1 and raw.get_num_connections() == 1), 15, 'configured ROS connections')
            if sender.poll() is not None:
                raise AssertionError('sender exited: ' + sender.stdout.read())
            # Allow declaration propagation before publishing the first sample.
            time.sleep(0.5)
            raw_bytes = bytes.fromhex('010000000000f07f')  # signaling NaN bit pattern
            raw_message = RawFloat(); raw_message._buff = raw_bytes
            count.publish(UInt32(123)); text.publish(String('raw \u2603')); raw.publish(raw_message)
            wait_until(lambda: len(received) >= 3, 5, 'first Zenoh samples')
            for value in range(200):
                count.publish(UInt32(value)); ignored.publish(String('ignored')); wrong.publish(String('wrong'))
                time.sleep(0.002)
            count.publish(UInt32(9999))
            wait_until(lambda: any(k == 'test/count' and b == serialized(UInt32(9999)) for k, b, _ in received), 5, 'latest throttled value')
            time.sleep(0.8)
            with lock: samples = list(received)
            counts = [(b, t) for k, b, t in samples if k == 'test/count']
            assert counts[0][0] == serialized(UInt32(123)), counts
            assert counts[-1][0] == serialized(UInt32(9999)), counts
            assert all(b == serialized(UInt32(v)) for b, v in [(b, int.from_bytes(b, 'little')) for b, _ in counts]), counts
            assert all(t2 - t1 >= 0.45 for (_, t1), (_, t2) in zip(counts, counts[1:])), counts
            assert len(counts) <= 3, counts
            assert any(k == 'test/text' and b == serialized(String('raw \u2603')) for k, b, _ in samples), samples
            assert any(k == 'test/raw' and b == raw_bytes for k, b, _ in samples), samples
            assert {k for k, _, _ in samples} == {'test/count', 'test/text', 'test/raw'}, samples
            assert ignored.get_num_connections() == 0 and wrong.get_num_connections() == 0
            sender.send_signal(signal.SIGINT)
            sender.wait(timeout=8)
            assert sender.returncode == 0, sender.stdout.read()
            sub.undeclare()
            print('PASS: ROS bytes, explicit mappings/type, latest-value throttling, shutdown')
        finally:
            if sender and sender.poll() is None:
                sender.terminate()
                try: sender.wait(timeout=5)
                except subprocess.TimeoutExpired: sender.kill(); sender.wait()
            if session: session.close()
            master.terminate()
            try: master.wait(timeout=5)
            except subprocess.TimeoutExpired: master.kill(); master.wait()


if __name__ == '__main__':
    main()
