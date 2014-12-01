import os
import re
import os.path
import json
import random
import hashlib
import logging
from collections import defaultdict
from grapy.core import Request
from datetime import datetime, timedelta
from grapy import engine


logger = logging.getLogger('huabot')


def get_env_redis_host():
    host_port = os.environ.get('REDIS_PORT', 'tcp://127.0.0.1:6379')
    host_port = host_port[6:]
    return host_port.split(':')

REDIS_HOST, REDIS_PORT = get_env_redis_host()

DB_PREFIX = os.environ.get("DB_PREFIX", "huabot")


def to_int(val):
    '''
    >>> to_int(2)
    2
    >>> to_int('2')
    2
    >>> to_int(b'2')
    2
    >>> to_int(2.2)
    2
    >>> to_int(b'2.2')
    2
    >>> to_int(b'2.2')
    2
    >>> to_int(True)
    1
    >>> to_int(False)
    0
    >>> to_int(None)
    Traceback (most recent call last):
      ...
    ValueError: invalid: could not convert None to int
    >>> to_int('a')
    Traceback (most recent call last):
      ...
    ValueError: invalid: could not convert a to int
    >>> to_int(b'a')
    Traceback (most recent call last):
      ...
    ValueError: invalid: could not convert b'a' to int
    '''
    if isinstance(val, (bytes, str)):
        if val.isdigit():
            return int(val)
        if isinstance(val, bytes):
            if val.count(b'.') == 1:
                if val.replace(b'.', b'').isdigit():
                    val = float(val)
                    return int(val)
        else:
            if val.count('.') == 1:
                if val.replace('.', '').isdigit():
                    val = float(val)
                    return int(val)
    if isinstance(val, (int, float, bool)):
        return int(val)

    raise ValueError('invalid: could not convert {} to int'.format(val))


def to_float(val):
    '''
    >>> to_float(2)
    2.0
    >>> to_float('2')
    2.0
    >>> to_float(b'2')
    2.0
    >>> to_float(2.2)
    2.2
    >>> to_float(b'2.2')
    2.2
    >>> to_float(b'2.2')
    2.2
    >>> to_float(True)
    1.0
    >>> to_float(False)
    0.0
    >>> to_float(None)
    Traceback (most recent call last):
      ...
    ValueError: invalid: could not convert None to float
    >>> to_float('a')
    Traceback (most recent call last):
      ...
    ValueError: invalid: could not convert a to float
    >>> to_float(b'a')
    Traceback (most recent call last):
      ...
    ValueError: invalid: could not convert b'a' to float
    '''
    if isinstance(val, (bytes, str)):
        if val.isdigit():
            return float(val)
        if isinstance(val, bytes):
            if val.count(b'.') == 1:
                if val.replace(b'.', b'').isdigit():
                    return float(val)
        else:
            if val.count('.') == 1:
                if val.replace('.', '').isdigit():
                    return float(val)
    if isinstance(val, (int, float, bool)):
        return float(val)

    raise ValueError('invalid: could not convert {} to float'.format(val))


def to_str(val, encoding='UTF-8'):
    '''
    >>> to_str(2)
    '2'
    >>> to_str(2.2)
    '2.2'
    >>> to_str('2.2')
    '2.2'
    >>> to_str('str')
    'str'
    >>> to_str(b'str')
    'str'
    '''
    if isinstance(val, (bytes, bytearray)):
        return str(val, encoding)
    return str(val)


def hash_url(url):
    h = hashlib.sha1()
    h.update(bytes(url, 'utf-8'))
    return h.hexdigest()


def submit_task(task):
    req = Request(task.url)
    req.group = task.index
    req.spider = task.spider
    req.unique = False
    engine.sched.push_req(req)


class SpecDict(dict):
    def __getitem__(self, key, default=None):
        return self.get(key, default)


def json_decode(data):
    ret = defaultdict(str)
    try:
        ret.update(json.loads(str(data, 'utf8')))
    except Exception:
        ret.update({'err': data})

    return ret


def random_delay(count, limit):
    now = datetime.now()
    day_end = datetime(now.year, now.month, now.day, 22, 0)
    tomorrow = datetime(now.year, now.month, now.day, 8, 0) + timedelta(1)
    if (limit and count >= limit) or day_end <= now:
        delay = tomorrow - now
        return delay.seconds
    if limit:
        time_remain = day_end - now
        remain = limit - count
        factor = max(5, int(time_remain.seconds / remain))
        minax = max(1, factor - random.randint(1, factor))
        return random.randint(minax, factor)

    return random.randint(10, 200)


def get_cls_name(klass):
    cls = klass.__class__
    cls_name = re.search("'([^']+)'", str(cls)).group(1)
    return cls_name
