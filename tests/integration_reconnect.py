#!/usr/bin/env python3
"""Exercise shared Zenoh router, matching, reconnection, and stop on isolated loopback."""
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
from xmlrpc.server import SimpleXMLRPCServer

import rospy
from std_msgs.msg import UInt32
import zenoh
from onboard_telemetry.sender import zenoh_config


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def wait_for(condition, label, timeout=15):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if condition():
            return
        time.sleep(0.05)
    raise AssertionError('timeout waiting for ' + label)


def sender_udp_sockets(pid):
    # Only sockets owned by this process, not every socket in its namespace.
    inodes = set()
    for fd in os.listdir('/proc/{}/fd'.format(pid)):
        try:
            target = os.readlink('/proc/{}/fd/{}'.format(pid, fd))
        except FileNotFoundError:
            continue
        if target.startswith('socket:['):
            inodes.add(target[8:-1])
    found = []
    for table in ('udp', 'udp6'):
        with open('/proc/{}/net/{}'.format(pid, table)) as stream:
            for row in list(stream)[1:]:
                fields = row.split()
                if fields[9] in inodes:
                    found.append((table, fields[1], fields[2]))
    return found


def cpu_seconds(pid):
    with open('/proc/{}/stat'.format(pid)) as stream:
        fields = stream.read().rsplit(')', 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK')


def main():
    ros_port, zenoh_port = free_port(), free_port()
    os.environ['ROS_MASTER_URI'] = 'http://127.0.0.1:{}'.format(ros_port)
    os.environ['ROS_HOSTNAME'] = '127.0.0.1'
    endpoint = 'udp/127.0.0.1:{}'.format(zenoh_port)
    config = {'connect': [endpoint], 'topics': [
        {'topic': '/reconnect/count', 'type': 'std_msgs/UInt32',
         'key': 'reconnect/count', 'max_hz': 20}]}
    conf = zenoh_config(zenoh, config['connect'])
    expected = {'mode': 'client', 'listen/endpoints': [], 'connect/endpoints': [endpoint],
                'scouting/multicast/enabled': False, 'scouting/gossip/enabled': False}
    for key, value in expected.items():
        assert json.loads(conf.get_json(key)) == value, key


    master = subprocess.Popen(['roscore', '-p', str(ros_port)], stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL)
    sender = None
    receiver = None
    router = None
    subscriber = None
    with tempfile.TemporaryDirectory() as tmp:
        try:
            def master_ready():
                try:
                    return xmlrpc.client.ServerProxy(os.environ['ROS_MASTER_URI']).getUri('/test')[0] == 1
                except OSError:
                    return False
            wait_for(master_ready, 'isolated ROS master')
            rospy.init_node('telemetry_reconnect_test', anonymous=True, disable_signals=True)
            topic = rospy.Publisher('/reconnect/count', UInt32, queue_size=1)
            path = os.path.join(tmp, 'config.json')
            with open(path, 'w') as stream:
                json.dump(config, stream)

            def start_sender(master_uri=None):
                executable = os.environ.get('TELEMETRY_BIN')
                command = [executable] if executable else [sys.executable, '-m', 'onboard_telemetry']
                env = os.environ.copy()
                if master_uri is not None:
                    env['ROS_MASTER_URI'] = master_uri
                process = subprocess.Popen(command + ['--config', path], stdout=subprocess.PIPE,
                                           stderr=subprocess.STDOUT, text=True, env=env)
                def node_registered():
                    try:
                        code, _, uri = xmlrpc.client.ServerProxy(os.environ['ROS_MASTER_URI']).lookupNode(
                            '/test', '/onboard_telemetry')
                        return code == 1 and xmlrpc.client.ServerProxy(uri).getPid('/test')[2] == process.pid
                    except (OSError, xmlrpc.client.Error):
                        return False
                wait_for(lambda: process.poll() is not None or node_registered(), 'sender ROS node')
                if process.poll() is not None:
                    raise AssertionError('sender exited: ' + process.stdout.read())
                return process

            def stop_sender(process, release_reply=None):
                started = time.monotonic()
                process.send_signal(signal.SIGINT)
                if release_reply is not None:
                    release_reply.set()
                process.wait(timeout=8)
                assert process.returncode == 0, process.stdout.read()
                assert time.monotonic() - started < 8
                wait_for(lambda: topic.get_num_connections() == 0, 'ROS subscriber cleanup')

            def open_router():
                router_config = zenoh.Config()
                for key, value in [('mode', '"router"'), ('listen/endpoints', json.dumps([endpoint])),
                                   ('scouting/multicast/enabled', 'false'),
                                   ('scouting/gossip/enabled', 'false')]:
                    router_config.insert_json5(key, value)
                return zenoh.open(router_config)

            def open_receiver():
                return zenoh.open(zenoh_config(zenoh, [endpoint]))

            def burst(start, count):
                for value in range(start, start + count):
                    topic.publish(UInt32(value))
                    time.sleep(0.002)

            def fresh(values, minimum):
                # Until matching propagation completes, only current input is eligible.
                end = time.monotonic() + 8
                value = minimum
                while time.monotonic() < end and not values:
                    topic.publish(UInt32(value))
                    value += 1
                    time.sleep(0.1)
                assert values and min(values) >= minimum, values

            router = open_router()
            sender = start_sender()
            assert sender_udp_sockets(sender.pid), 'sender has no UDP socket'
            before = cpu_seconds(sender.pid)
            burst(100, 150)
            time.sleep(2.2)
            assert topic.get_num_connections() == 0, 'ROS subscribed without a Zenoh consumer'
            assert cpu_seconds(sender.pid) - before < 1.5, 'unexpected CPU without display demand'
            assert sender.poll() is None

            receiver = open_receiver()
            burst(500, 150)  # Connected, but no matching Zenoh subscriber.
            assert topic.get_num_connections() == 0, 'peer alone must not demand ROS data'
            values = []
            subscriber = receiver.declare_subscriber('reconnect/count',
                lambda sample: values.append(int.from_bytes(sample.payload.to_bytes(), 'little')))
            wait_for(lambda: topic.get_num_connections() == 1, 'matching ROS subscription')
            assert not values, 'unmatched sample was replayed: {}'.format(values)
            fresh(values, 1000)

            # The selected router disappears and returns. Both existing SDK
            # sessions, not new Workflow Runs or a product retry loop, own
            # their reconnection and matching changes.
            router.close(); router = None
            wait_for(lambda: topic.get_num_connections() == 0, 'ROS unsubscribe after router loss', timeout=25)
            values.clear()
            router = open_router()
            wait_for(lambda: topic.get_num_connections() == 1, 'matching after router return', timeout=25)
            fresh(values, 1500)

            subscriber.undeclare(); subscriber = None
            wait_for(lambda: topic.get_num_connections() == 0, 'ROS unsubscribe on loss of matching')
            receiver.close(); receiver = None
            burst(2000, 150)
            assert sender.poll() is None
            receiver = open_receiver()
            burst(3000, 150)  # Reconnected, still no matching subscriber.
            assert topic.get_num_connections() == 0, 'reconnected peer alone must not demand ROS data'
            values = []
            subscriber = receiver.declare_subscriber('reconnect/count',
                lambda sample: values.append(int.from_bytes(sample.payload.to_bytes(), 'little')))
            wait_for(lambda: topic.get_num_connections() == 1, 'ROS resubscribe after matching')
            assert not values, 'disconnected/unmatched sample was replayed: {}'.format(values)
            fresh(values, 4000)
            assert sender_udp_sockets(sender.pid), 'sender UDP socket missing'
            stop_sender(sender); sender = None
            subscriber.undeclare(); subscriber = None
            receiver.close(); receiver = None

            # Stop also works when a connected client has no subscriber.
            sender = start_sender()
            stop_sender(sender); sender = None
            # Hold a successful native unregister reply across the process's
            # SIGINT. rospy shutdown uses the same cached ROS master proxy.
            with SimpleXMLRPCServer(('127.0.0.1', 0), logRequests=False) as proxy:
                reply_pending = threading.Event()
                release_reply = threading.Event()
                def forward(method, params):
                    with xmlrpc.client.ServerProxy(os.environ['ROS_MASTER_URI']) as upstream:
                        result = getattr(upstream, method)(*params)
                    if method == 'unregisterSubscriber' and params[:2] == ('/onboard_telemetry', '/reconnect/count'):
                        assert result[0] == 1
                        reply_pending.set()
                        assert release_reply.wait(5), 'held unregister reply was never released'
                    return result
                proxy._dispatch = forward
                service = threading.Thread(target=proxy.serve_forever)
                service.start()
                try:
                    receiver = open_receiver()
                    subscriber = receiver.declare_subscriber('reconnect/count', lambda sample: None)
                    sender = start_sender('http://127.0.0.1:{}'.format(proxy.server_address[1]))
                    wait_for(lambda: topic.get_num_connections() == 1, 'ROS subscriber before overlapping stop')
                    subscriber.undeclare(); subscriber = None
                    assert reply_pending.wait(5), 'no native unregister request'
                    stop_sender(sender, release_reply); sender = None
                finally:
                    release_reply.set()
                    proxy.shutdown()
                    service.join()

            print('PASS: shared UDP router loss/return, consumer reconnect, no-match drop, bounded idle, and stop during ROS unregistration')
        finally:
            if sender and sender.poll() is None:
                sender.terminate()
                try: sender.wait(timeout=5)
                except subprocess.TimeoutExpired: sender.kill(); sender.wait()
            if subscriber:
                subscriber.undeclare()
            if receiver:
                receiver.close()
            if router:
                router.close()
            master.terminate()
            try: master.wait(timeout=5)
            except subprocess.TimeoutExpired: master.kill(); master.wait()


if __name__ == '__main__':
    main()
