import argparse
import sys

from .config import ConfigError, load_config


def main():
    parser = argparse.ArgumentParser(description='Forward selected ROS1 topics to Zenoh')
    parser.add_argument('--config', required=True, help='JSON configuration file')
    args = parser.parse_args()
    try:
        config = load_config(args.config)
        import rospy
        from .sender import run
        rospy.init_node('onboard_telemetry', anonymous=False, disable_rosout=False)
        run(config)
    except (ConfigError, ValueError, OSError, ImportError) as exc:
        print('onboard-telemetry: {}'.format(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
