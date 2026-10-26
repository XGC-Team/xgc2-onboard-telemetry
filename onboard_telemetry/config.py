"""Read and validate the explicit telemetry configuration."""
import json
import math
import re


class ConfigError(ValueError):
    pass


def _object(value, fields, where):
    if not isinstance(value, dict) or set(value) != set(fields):
        raise ConfigError('{} must contain exactly {}'.format(where, ', '.join(fields)))


def _string(value, where):
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ConfigError('{} must be a non-empty string without surrounding whitespace'.format(where))
    return value


def load_config(path):
    with open(path, encoding='utf-8') as stream:
        return parse_config(json.load(stream))


def parse_config(data):
    _object(data, ('connect', 'topics'), 'config')
    endpoints = data['connect']
    if not isinstance(endpoints, list) or not endpoints:
        raise ConfigError('connect must be a non-empty endpoint list')
    for endpoint in endpoints:
        _string(endpoint, 'endpoint')
    topics = data['topics']
    if not isinstance(topics, list) or not topics:
        raise ConfigError('topics must be a non-empty list')
    seen_topics, seen_keys = set(), set()
    for item in topics:
        _object(item, ('topic', 'type', 'key', 'max_hz'), 'topic mapping')
        topic = _string(item['topic'], 'topic')
        message_type = _string(item['type'], 'type')
        key = _string(item['key'], 'key')
        if not re.fullmatch(r'/[A-Za-z][A-Za-z0-9_]*(?:/[A-Za-z][A-Za-z0-9_]*)*', topic):
            raise ConfigError('topic must be an absolute ROS topic: {}'.format(topic))
        if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]*/[A-Za-z][A-Za-z0-9_]*', message_type):
            raise ConfigError('type must be package/Message: {}'.format(message_type))
        if '*' in key or key.startswith('/') or key.endswith('/') or '//' in key:
            raise ConfigError('key must be a concrete Zenoh key: {}'.format(key))
        hz = item['max_hz']
        if isinstance(hz, bool) or not isinstance(hz, (int, float)) or not math.isfinite(hz) or not 0 < hz <= 1000 or hz < 1e-6:
            raise ConfigError('max_hz must be between 0.000001 and 1000')
        if topic in seen_topics or key in seen_keys:
            raise ConfigError('topics and keys must be unique')
        seen_topics.add(topic)
        seen_keys.add(key)
    return data
