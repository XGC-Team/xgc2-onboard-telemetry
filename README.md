# Onboard telemetry

A ROS 1 Noetic sender forwards explicitly configured local topics to Zenoh. It
subscribes only to the listed topics and does not discover a ROS graph. Each
mapping names one ROS topic, its installed ROS message type, a complete Zenoh
key and its maximum send rate.

## Requirements

- Python 3.8 and ROS 1 Noetic, including `rospy`, `roslib` and the configured
  message packages.
- `eclipse-zenoh` Python SDK **1.5.1**, installed in the runtime environment.
  The product's installer does not install dependencies. The SDK's official
  wheels include Python 3.8 and Linux x86-64/ARM64 support; choose the wheel
  matching the target's architecture and libc. See [SDK package and files](https://pypi.org/project/eclipse-zenoh/1.5.1/).
- A Zenoh router or peer reachable at the configured endpoint. The sender can
  start before the receiver and reconnect after it restarts. It uses Zenoh peer
  mode with no listening endpoint, connects only to the explicitly configured
  endpoints, and disables multicast and gossip scouting. Zenoh retries connections
  in the background with backoff from one to four seconds.

## Install and run

```bash
# Dependencies are provided by the runtime image or host.
sudo ./install.sh /usr/local
source /opt/ros/noetic/setup.bash
onboard-telemetry --config /path/to/telemetry.json
```

For callers that already hold the configuration as JSON, pass it as one
argument with `--config-json` instead of `--config`; the two options are mutually
exclusive.

The installer copies only this product and its entrypoint. An absolute prefix
may be supplied without sudo when writable. Source any additional ROS workspace
that contains configured message types before starting the sender.

To build the product's architecture-independent Debian package:

```bash
./build-deb.sh                         # writes /output/packages/*.deb
./build-deb.sh --output-dir /path/to/packages
```

The package installs the same entrypoint and Python files under `/usr`. Its
Debian dependencies are Python, Noetic `rospy` and `roslib`. The Zenoh SDK is
provided by the runtime baseline, not by this package or by its installation
scripts; its required version is recorded in `requirements-runtime.txt`.
Installing or removing this package does not manage ROS, the SDK, or services.
The package also installs the `xgc2-onboard-telemetry` XGC process definition.
It accepts a `configuration` JSON object, `rosMasterUri` and `rosIp`, sources
the Noetic setup, and stops with SIGINT. The process supervisor serializes the
object directly into one `--config-json` argument. The definition declares no
control service.

Example configuration is in [examples/telemetry.json](examples/telemetry.json).
`connect` must contain one or more Zenoh endpoint strings. Each topic and key
must be unique; topics are absolute ROS names, types use `package/Message`,
keys are concrete Zenoh keys, and `max_hz` is greater than zero and at most
1000. There are no default topic mappings. The endpoint in the example is a
local example; use the receiving endpoint for the actual deployment.

## Wire and scheduling

The Zenoh payload is the original ROS 1 serialized message bytes, without a
length prefix, conversion or private envelope. The subscriber and receiver
must agree out of band on each key and its ROS message type; the sender does
not transmit type metadata. ROS type and MD5 are checked during the ROS
connection handshake. The payload can be decoded using the agreed ROS message
type's standard deserializer.

A native Zenoh matching listener tracks whether each key has a consumer. Without
a match, incoming raw ROS bytes are discarded; no Zenoh publication is attempted
and pending bytes are cleared when the match disappears. Only new input is sent
after a consumer returns. This does not unsubscribe from the local ROS topic;
ROS still serializes and transports its messages locally. Matching changes and
a send can race, so an in-flight publication can still be dropped on disconnect.

Each ROS subscription uses `queue_size=1`. A single scheduler holds at most
one unsent value per mapping, overwrites it with newer input, and uses monotonic
wall time to enforce the maximum rate. It sends a pending value once, with no
periodic replay, and does not create a worker thread per topic. The Zenoh
publisher uses drop-on-congestion; this is a live telemetry stream rather than
an archive or a delivery guarantee.

## Tests

```bash
python3 -m unittest discover -s tests -p 'test_*.py' -v
# In a ROS Noetic environment with the SDK above and a sourced ROS setup:
python3 tests/integration_ros_zenoh.py
python3 tests/integration_reconnect.py
# In a disposable root-owned Noetic container with the SDK available:
./tests/test_deb.sh
```

The integration test starts a temporary local ROS master, ROS publishers and a
Zenoh peer on loopback, then checks exact serialized bytes, type matching,
explicit mappings, rate limiting, latest-value delivery and shutdown. It does
not use a running ROS master or any existing Zenoh session.
