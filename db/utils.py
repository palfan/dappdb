# coding: utf-8

import json


def to_bytes(text, encoding='utf-8'):
    if isinstance(text, bytes):
        return text
    return text.encode(encoding)


def encode(value):
    if not value:
        return value  # in case [], 0, None
    if isinstance(value, unicode):
        value = to_bytes(value)
    if isinstance(value, str):
        if value.startswith(('[', '{')):
            try:
                value = json.loads(value, encoding='utf-8')
            except ValueError:
                pass
    if isinstance(value, list):
        value = [encode(i) for i in value]
    if isinstance(value, dict):
        value = {to_bytes(k): encode(v) for k, v in value.iteritems()}
    return value
