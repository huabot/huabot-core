from .utils import to_int, to_str, REDIS_PORT, REDIS_HOST, DB_PREFIX, hash_url
import redis
from time import time
from collections import defaultdict
import json
from grapy.core import Request
from datetime import datetime


class DB(object):

    def __init__(self, prefix=DB_PREFIX):
        self._db = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT)
        self._prefix = prefix

    def get_object(self, key):
        key = self._prefix + ":" + key
        data = self._db.get(key)
        ret = defaultdict(str)
        if data:
            data = json.loads(to_str(data))
            ret.update(data)

        return ret

    def set_object(self, key, obj):
        key = self._prefix + ":" + key
        self._db.set(key, json.dumps(obj.copy()))

    def del_object(self, key):
        key = self._prefix + ":" + key
        self._db.delete(key)

    def next_sequence(self, name):
        key = self._prefix + ":sequence:" + name
        ret = self._db.incr(key, 1)
        return to_int(ret)

    def add_index(self, name, member, score=1):
        key = ":".join([self._prefix, 'index', name])
        self._db.zadd(key, score, member)

    def get_index(self, name, member):
        key = ":".join([self._prefix, 'index', name])
        try:
            ret = self._db.zscore(key, member)
            ret = to_int(ret)
        except:
            ret = 0

        return ret

    def range_index(self, name, start=0, stop=-1, reverse=False):
        key = ":".join([self._prefix, 'index', name])
        try:
            if reverse:
                data = self._db.zrevrange(key, start, stop)
            else:
                data = self._db.zrange(key, start, stop)
        except:
            return []

        return [(to_str(index), self._db.zscore(key, to_str(index)))
                for index in data]

    def count_index(self, name):
        key = ":".join([self._prefix, 'index', name])
        count = self._db.zcard(key)
        return int(count)

    def drop_index(self, name):
        key = ":".join([self._prefix, 'index', name])
        self._db.delete(key)

    def delete_index(self, name, member):
        key = ":".join([self._prefix, 'index', name])
        self._db.zrem(key, member)

    def execute(self, cmd, key, *args):
        key = self._prefix + ":" + key
        func = getattr(self._db, cmd.lower())
        if func:
            try:
                return func(key, *args)
            except:
                pass
        if cmd.lower().find("range") > -1:
            return []
        return None

db = DB()

get_object = db.get_object
set_object = db.set_object
del_object = db.del_object
next_sequence = db.next_sequence
add_index = db.add_index
get_index = db.get_index
range_index = db.range_index
count_index = db.count_index
drop_index = db.drop_index
delete_index = db.delete_index
execute = db.execute


class Index(object):

    def __init__(self, name, member=None, score=None):
        self.name = name
        self.member = member
        self._score = score

    @property
    def score(self):
        if self.member is None:
            raise IndexError("IndexError: Index [{}] member is None".format(
                self.name))

        if self._score is None:
            self._score = db.get_index(self.name, self.member)

        return self._score

    @score.setter
    def score(self, value):
        self._score = value

    def save(self):
        if self.member is None:
            raise IndexError(
                "IndexError: Index [{}] member is None".format(self.name))

        db.add_index(self.name, self.member, self.score)

    def delete(self):
        if self.member is None:
            raise IndexError(
                "IndexError: Index [{}] member is None".format(self.name))

        db.delete_index(self.name, self.member)

    @classmethod
    def get(self, name, member):
        score = db.get_index(name, member)

        return Index(name, member, score)

    @classmethod
    def range(self, name, start=0, stop=-1, reverse=False):
        ret = db.range_index(name, start, stop, reverse)
        return [Index(name, idx[0], idx[1]) for idx in ret]

    @classmethod
    def drop(self, name):
        db.drop_index(name)

    @classmethod
    def count(self, name):
        return db.count_index(name)


class Table(object):
    table_name = "table"
    unique_columns = []
    index_columns = []

    def __init__(self, index=None, payload=None):
        self._index = index
        self._payload = defaultdict(str)
        if payload:
            self._payload.update(payload)

    @property
    def index(self):
        if self._index:
            return self._index

        self._index = self._payload[self.table_name + '_id']

        return self._index

    @index.setter
    def index(self, value):
        self._index = value
        self._payload[self.table_name + '_id'] = value

    @property
    def payload(self):
        if self.index and not self._payload:
            self._payload = db.get_object(self.key())

        return self._payload

    @payload.setter
    def payload(self, payload):
        self._payload = payload

    def save(self):
        if not self._payload:
            raise ValueError("Table: {} value is None".format(self.table_name))

        if not self.index:
            for columns in self.unique_columns:
                columns = columns.split(" ")
                members = [to_str(self._payload[column]) for column in columns]
                index = Index.get(":".join([self.table_name] + columns),
                                  ':'.join(members))
                if index.score > 0:
                    raise ValueError(
                        "Table: {} duplicate columns: {} value: {}".format(
                            self.table_name, columns, members))
            self.index = db.next_sequence(self.table_name)
        else:
            for columns in self.unique_columns:
                columns = columns.split(" ")
                members = [to_str(self._payload[column]) for column in columns]
                index = Index.get(":".join([self.table_name] + columns),
                                  ':'.join(members))
                if index.score > 0 and index.score != self.index:
                    raise ValueError(
                        "Table: {} duplicate columns: {} value: {}".format(
                            self.table_name, columns, members))

            old = db.get_object(self.key())
            for columns in self.unique_columns:
                columns = columns.split(" ")
                is_df = False
                for column in columns:
                    if old[column] != self._payload[column]:
                        is_df = True

                if is_df:
                    members = [to_str(old[column]) for column in columns]
                    Index.get(":".join([self.table_name] + columns),
                              ':'.join(members)).delete()

            for columns in self.index_columns:
                columns = columns.split(" ")
                is_df = False
                for column in columns:
                    if old[column] != self._payload[column]:
                        is_df = True
                if is_df:
                    members = [to_str(old[column]) for column in columns]
                    keys = [self.table_name]
                    for c, m in zip(columns, members):
                        keys.append(c)
                        keys.append(m)

                    idx = Index(":".join(keys))
                    idx.member = to_str(self.index)
                    idx.delete()

        db.set_object(self.key(), self._payload)
        idx = Index(self.table_name)
        idx.member = to_str(self.index)
        idx.score = self.index
        idx.save()

        for columns in self.unique_columns:
            columns = columns.split(" ")
            members = [to_str(self._payload[column]) for column in columns]
            idx = Index(":".join([self.table_name] + columns))
            idx.member = ':'.join(members)
            idx.score = self.index
            idx.save()

        for columns in self.index_columns:
            columns = columns.split(" ")
            members = [to_str(self._payload[column]) for column in columns]
            keys = [self.table_name]
            for c, m in zip(columns, members):
                keys.append(c)
                keys.append(m)
            idx = Index(":".join(keys))
            idx.member = to_str(self.index)
            idx.score = self.index
            idx.save()

    @classmethod
    def get(self, index):
        return self(index)

    @classmethod
    def get_by_uniq(self, column, member=None):
        key = ":".join([self.table_name, column])

        def _get_by_uniq(member):
            index = Index.get(key, member)
            if index.score > 0:
                return self(index.score)
            return None

        if member:
            return _get_by_uniq(member)
        else:
            return _get_by_uniq

    @classmethod
    def range(self, column, start=0, stop=-1, reverse=False):
        idxs = Index.range(":".join([self.table_name, column]),
                           start, stop, reverse)
        return idxs

    @classmethod
    def range_by_index(self, column, value=None, start=0, stop=-1,
                       reverse=False):
        def _range_by_index(value, start=0, stop=-1, reverse=False):
            idxs = Index.range(
                ":".join([self.table_name, column, to_str(value)]),
                start, stop, reverse)
            return idxs

        if value:
            return _range_by_index(value, start, stop, reverse)
        else:
            return _range_by_index

    @classmethod
    def count_by_index(self, column, value=None):
        def _range_by_index(value):
            count = Index.count(":".join([self.table_name, column,
                                          to_str(value)]))
            return count

        if value:
            return _range_by_index(value)
        else:
            return _range_by_index

    def __getattr__(self, key):
        if not self._payload:
            self._payload = db.get_object(self.key())

        return self._payload[key]

    def key(self):
        return ":".join([self.table_name, to_str(self.index)])

    def delete(self):
        idx = Index(self.table_name)
        idx.member = to_str(self.index)
        idx.delete()

        for column in self.unique_columns:
            idx = Index(":".join([self.table_name, column]))
            idx.member = to_str(self.payload[column])
            idx.delete()

        for column in self.index_columns:
            idx = Index(":".join([self.table_name, column,
                                  to_str(self.payload[column])]))
            idx.member = to_str(self.index)
            idx.delete()

        db.del_object(self.key())

    @classmethod
    def count(self):
        return Index.count(self.table_name)


def init_table(table):
    for column in table.unique_columns:
        setattr(table, "get_by_" + column, table.get_by_uniq(column, False))

    for column in table.index_columns:
        setattr(
            table, "range_by_" + column, table.range_by_index(column, False))

    for column in table.index_columns:
        setattr(
            table, "count_by_" + column, table.count_by_index(column, False))


class Countable(object):

    dtypes = ['year', 'month', 'day', 'hour', 'minute']

    @classmethod
    def p_key(self):
        return ":".join([self.table_name, "succeed_count"])

    @classmethod
    def pu_key(self, user_id):
        return ":".join([self.table_name, "user_id", to_str(user_id),
                         "succeed_count"])

    def incr_succeed_count(self):
        self._incr(self.p_key(), to_str(self.index))
        if self.table_name != 'user':
            self._incr(self.pu_key(self.payload['user_id']),
                       to_str(self.index))
        now = datetime.now()
        members = [
            "%s" % now.year,
            "%s:%s" % (now.year, now.month),
            "%s:%s:%s" % (now.year, now.month, now.day),
            "%s:%s:%s:%s" % (now.year, now.month, now.day, now.hour),
            "%s:%s:%s:%s:%s" % (
                now.year, now.month, now.day, now.hour, now.minute)
        ]

        for dtype, member in zip(self.dtypes, members):
            key = ':'.join([self.table_name, to_str(self.index), dtype,
                            'succeed_count'])
            self._incr(key, member)

    def init_succeed_count(self):
        db.execute("zadd", self.p_key(), 0, to_str(self.index))
        if self.table_name != 'user':
            db.execute("zadd", self.pu_key(self.payload['user_id']), 0,
                       to_str(self.index))

    def _incr(self, key, member):
        try:
            ret = db.execute("zincrby", key, member, 1)
        except:
            ret = db.execute("zadd", key, 1, member)

        return to_int(ret)

    @property
    def succeed_count(self):
        try:
            ret = db.execute('zscore', self.p_key(), to_str(self.index))
            if not ret:
                ret = db.execute(
                    'zscore', self.pu_key(self.payload['user_id']),
                    to_str(self.index))
                if ret:
                    db.execute(
                        'zadd', self.p_key(), to_int(ret), to_str(self.index))
            ret = to_int(ret)
        except:
            ret = 0

        return ret

    def get_time_succeed_count(self, member):
        member = member.replace('-', ':').replace(' ', ':')
        dtype = self.dtypes[member.count(':')]
        key = ':'.join([self.table_name, to_str(self.index), dtype,
                        'succeed_count'])
        try:
            ret = db.execute('zscore', key, member)
            ret = to_int(ret)
        except:
            ret = 0

        return ret

    @classmethod
    def range_succeed_count(self, start=0, stop=-1, reverse=False):
        if reverse:
            data = db.execute('zrevrange', self.p_key(), start, stop)
        else:
            data = db.execute('zrange', self.p_key(), start, stop)
        return [(to_str(index),
                 db.execute('zscore', self.p_key(), to_str(index)))
                for index in data]

    @classmethod
    def range_user_succeed_count(self, user_id, start=0, stop=-1,
                                 reverse=False):
        if reverse:
            data = db.execute('zrevrange', self.pu_key(user_id), start, stop)
        else:
            data = db.execute('zrange', self.pu_key(user_id), start, stop)
        return [(to_str(index),
                 db.execute('zscore', self.pu_key(user_id), to_str(index)))
                for index in data]

    def del_succeed_count(self):
        for dtype in self.dtypes:
            key = ':'.join([self.table_name, to_str(self.index), dtype,
                            'succeed_count'])
            db.del_object(key)

        db.execute("zrem", self.p_key(), to_str(self.index))

        db.execute("zrem", self.pu_key(self.payload['user_id']),
                   to_str(self.index))


class Schedable(object):

    def sched_later(self, timeout):
        sched_at = time() + timeout
        sched_at = int(sched_at)
        db.execute('zadd', self.table_name + ':sched', sched_at, self.index)

    def sched_now(self):
        self.sched_later(0)

    @classmethod
    def sched(self):
        data = db.execute('zrange', self.table_name + ':sched', 0, 0)
        if data:
            data = int(data[0])
            return self(data)

        return None

    def remove_sched(self):
        db.execute("zrem", self.table_name + ':sched', self.index)

    @property
    def sched_at(self):
        if hasattr(self, '_sched_at'):
            if self._sched_at:
                return self._sched_at

        self._sched_at = db.execute('zscore', self.table_name + ':sched',
                                    self.index)
        return self._sched_at


class Task(Table, Countable, Schedable):
    table_name = "task"
    unique_columns = ["hash_url"]
    index_columns = ["user_id"]

    def delete(self):
        Table.delete(self)
        self.del_succeed_count()
        self.remove_sched()
        key = "{}:{}:link:uniq".format(self.table_name, self.index)
        db.del_object(key)

    def save(self):
        is_new = False
        if not self.index:
            is_new = True

        Table.save(self)

        if is_new:
            self.init_succeed_count()
            key = "{}:{}:link:uniq".format(self.table_name, self.index)
            db.execute('sadd', key, 'haha')

    @property
    def subscribed(self):
        ret = db.execute("smembers", "{}:{}:subscribed".format(self.table_name,
                                                               self.index))
        if ret:
            return [int(x) for x in ret]

        return []

    def subscribed_new(self, robot_id):
        db.execute("sadd", "{}:{}:subscribed".format(
            self.table_name, self.index), robot_id)

    def subscribed_rm(self, robot_id):
        db.execute("srem", "{}:{}:subscribed".format(
            self.table_name, self.index), robot_id)

    def subscribed_count(self):
        return db.execute("scard", "{}:{}:subscribed".format(
            self.table_name, self.index))

    def link_push(self, hash_url):
        key = "{}:{}:link".format(self.table_name, self.index)
        db.execute("lpush", key, hash_url)

    def link_pop(self):
        key = "{}:{}:link".format(self.table_name, self.index)
        ret = db.execute("rpop", key)
        if ret:
            return str(ret, 'utf-8')
        return None

    def link_count(self):
        return db.execute(
            "llen", "{}:{}:link".format(self.table_name, self.index))

    def link_drop(self):
        return db.execute(
            "delete", "{}:{}:link".format(self.table_name, self.index))

    def incr_visit(self):
        ret = db.execute(
            "incr", "{}:{}:visit".format(self.table_name, self.index))
        return ret

    @property
    def visit(self):
        ret = db.execute(
            "get", "{}:{}:visit".format(self.table_name, self.index))
        if ret:
            return int(ret)
        else:
            return 0

    @property
    def visit_item(self):
        return Item.visit(self.index)

    def has(self, hash_url):
        key = "{}:{}:link:uniq".format(self.table_name, self.index)
        if db.execute("sismember", key, hash_url):
            return True

        db.execute("sadd", key, hash_url)
        return False

    def clear_uniq(self):
        key = "{}:{}:link:uniq".format(self.table_name, self.index)
        db.del_object(key)
        db.execute("sadd", key, "haha")

        while True:
            hash_url = self.link_pop()
            if not hash_url:
                break

            link = Link(hash_url)
            link.delete()

    def item_count(self):
        return Item.count_by_index("task_id", self.index)


init_table(Task)


class Robot(Table, Countable, Schedable):
    table_name = "robot"
    unique_columns = ["email"]
    index_columns = ["user_id", "alive user_id"]

    def delete(self):
        Table.delete(self)
        self.del_succeed_count()
        self.remove_sched()
        self.drop_item()

    def save(self):
        is_new = False
        if not self.index:
            is_new = True

        Table.save(self)

        if is_new:
            self.init_succeed_count()
            self.set_item('haha')

    def set_item(self, hash_url):
        db.execute('sadd', "{}:{}:item".format(self.table_name, self.index),
                   hash_url)

        db.execute('incr', "{}:{}:item:visit".format(self.table_name,
                                                     self.index))

    def del_item(self, hash_url):
        db.execute('srem', "{}:{}:item".format(self.table_name, self.index),
                   hash_url)

    def has_item(self, hash_url):
        retval = db.execute('sismember', "{}:{}:item".format(
            self.table_name, self.index), hash_url)

        if retval:
            db.execute('incr', "{}:{}:item:visit".format(self.table_name,
                                                         self.index))

        return retval

    def drop_item(self):
        db.execute('delete', "{}:{}:item".format(self.table_name, self.index))

    @property
    def visit_item(self):
        return db.execute('get', "{}:{}:item:visit".format(self.table_name, self.index))


init_table(Robot)


class User(Table, Countable):
    table_name = "user"
    unique_columns = ["name"]

    def delete(self):
        Table.delete(self)
        self.del_succeed_count()

    @property
    def robot_count(self):
        return Robot.count_by_index("user_id", to_str(self.index))

    @property
    def alive_robot_count(self):
        return Robot.count_by_index("alive:True:user_id", to_str(self.index))

    @property
    def task_count(self):
        key = ':'.join([Task.table_name, 'user_id', to_str(self.index)])
        return Index.count(key)


init_table(User)


class Item(Table):
    table_name = 'item'
    unique_columns = ["hash_url"]
    index_columns = ['task_id']

    def save(self):
        Table.save(self)
        self.add_queue()

    def delete(self):
        Table.delete(self)
        self.del_queue()

    @classmethod
    def has(self, hash_url):
        key = "{}:uniq".format(self.table_name)
        if db.execute("sismember", key, hash_url):
            return True

        db.execute("sadd", key, hash_url)
        return False

    def add_queue(self):
        key = "{}:{}:queue".format(self.table_name, self.payload['task_id'])
        db.execute("sadd", key, self.index)

    def incr_visit(self):
        key = "{}:{}:visit".format(self.table_name, self._payload['task_id'])
        db.execute('incr', key)

    @classmethod
    def visit(self, task_id):
        key = "{}:{}:visit".format(self.table_name, task_id)
        return db.execute('get', key)

    @classmethod
    def pop_queue(self, task_id):
        key = "{}:{}:queue".format(self.table_name, task_id)
        ret = db.execute('spop', key)
        if ret:
            return int(ret)

        return None

    def del_queue(self):
        key = "{}:{}:queue".format(self.table_name, self.payload['task_id'])
        db.execute("srem", key, self.index)

    @classmethod
    def drop_queue(self, task_id):
        key = "{}:{}:queue".format(self.table_name, task_id)
        db.execute("delete", key)
        Index.drop("{}:{}:{}".format(self.table_name, 'task_id', task_id))

db.execute("sadd", "{}:uniq".format(Item.table_name), "haha")

init_table(Item)


class Link(object):
    table_name = 'link'

    def __init__(self, index=None, req=None):
        self._req = req
        self._index = index

    @property
    def index(self):
        if not self._index and self._req:
            self._index = hash_url(self._req.url)

        return self._index

    @index.setter
    def index(self, value):
        self._index = value

    @property
    def req(self):
        if self._index and not self._req:
            data = db.execute("GET", "{}:{}".format(self.table_name,
                                                    self.index))
            if data:
                self._req = Request.build(data)

        return self._req

    def save(self):
        db.execute("SET", "{}:{}".format(self.table_name, self.index),
                   bytes(self.req))

    def delete(self):
        db.del_object("{}:{}".format(self.table_name, self.index))

    @classmethod
    def get(self, hash_url):
        return self(hash_url)

    @classmethod
    def has(self, hash_url):
        key = "{}:uniq".format(self.table_name)
        if db.execute("sismember", key, hash_url):
            return True

        db.execute("sadd", key, hash_url)
        return False


db.execute("sadd", "{}:uniq".format(Link.table_name), "haha")
