import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor

from .config import ConfigError, load_config, parse_config


def main():
    parser = argparse.ArgumentParser(description='Forward selected ROS1 topics to Zenoh')
    config_input = parser.add_mutually_exclusive_group(required=True)
    config_input.add_argument('--config', help='JSON configuration file')
    config_input.add_argument('--config-json', help='JSON configuration as a single argument')
    args = parser.parse_args()
    try:
        config = load_config(args.config) if args.config is not None else parse_config(json.loads(args.config_json))
        import rospy
        from .sender import run
        rospy.init_node('onboard_telemetry', anonymous=False, disable_rosout=False)
        # rospy handles SIGINT/SIGTERM on the main thread. Registration RPCs
        # hold a non-reentrant proxy lock which rospy also needs on shutdown.
        with ThreadPoolExecutor(max_workers=1) as worker:
            worker.submit(run, config).result()
    except (ConfigError, ValueError, OSError, ImportError) as exc:
        print('onboard-telemetry: {}'.format(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
