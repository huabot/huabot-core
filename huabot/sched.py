from grapy.core import BaseScheduler
from grapy.core.exceptions import RetryRequest
import asyncio
from .utils import hash_url, SpecDict, random_delay, logger, get_cls_name
from . import db
import random
from time import time
from datetime import datetime
from .robot import RobotError
from aio_periodic import Worker, Pool
from grapy.utils import import_module
import signal
import os


class RobotBased(object):
    def __init__(self, pool_size):
        self.started = False
        self.connect_lock = asyncio.Lock()
        self.grabJob_lock = asyncio.Lock()
        self.alive = True
        self.tasks = []
        self.pool = Pool(self.init_worker, pool_size, 500)

    def init_worker(self):
        client = Worker()
        client.add_server(os.environ["PERIODIC_PORT"])
        yield from client.connect()
        yield from client.add_func("process_robot")
        return client

    def run(self, job):

        if job.func_name != 'process_robot':
            return

        try:
            robot = db.Robot(int(job.name))
            if not robot.payload:
                yield from job.done()
                return
        except Exception as e:
            logger.exception(e)
            yield from job.done()
            return

        try:
            yield from self.robot_main(robot)
        finally:
            yield from self.done(job)

    def done(self, job):
        now = datetime.now()
        day = "%s-%s-%s" % (now.year, now.month, now.day)
        robot = db.Robot(int(job.name))
        day_succeed_count = robot.get_time_succeed_count(day)
        delay = random_delay(day_succeed_count, robot.day_limit)
        if robot.alive:
            robot.sched_later(delay)
            yield from job.sched_later(delay)
        else:
            yield from job.done()

    def start(self):
        if self.started:
            return
        asyncio.Task(self._start())

    def _start(self):
        self.started = True
        while self.alive:
            client = None
            try:
                client = yield from self.pool.get()
            except Exception as e:
                yield from asyncio.sleep(5)
                print(e)

            if not client:
                continue

            job = None

            try:
                job = yield from client.grabJob()
            except Exception as e:
                logger.exception(e)
                self.pool.release()
                continue

            if not job:
                logger.warning("NoJOB, waiting...")
                yield from asyncio.sleep(5)
                self.pool.release()
                continue

            yield from self._sem.acquire()
            task = asyncio.Task(self.run(job))
            task.add_done_callback(lambda t: self._sem.release())
            task.add_done_callback(lambda t: self.tasks.remove(t))
            task.add_done_callback(lambda t: self.pool.release())
            self.tasks.append(task)

        self.started = False

    def signal_handler(self):
        if not self.alive:
            return
        self.alive = False

        print("signal close")

        task = asyncio.Task(asyncio.wait(self.tasks))
        task.add_done_callback(lambda t: self.engine.shutdown())
        # self.loop.call_later(600, self.engine.shutdown)


class UniqScheduler(BaseScheduler):
    def __init__(self, tasks=4, loop=None):
        BaseScheduler.__init__(self)
        self.loop = loop
        if not self.loop:
            self.loop = asyncio.get_event_loop()

        self._sem = asyncio.Semaphore(tasks)
        self._item_lock = asyncio.Lock()

    def push_req(self, req):
        key = hash_url(req.url)
        group = int(req.group)
        task = db.Task(group)
        if not task.payload:
            return

        if task.has(key) and req.unique:
            return

        task.link_push(key)
        task.incr_visit()

        callback_args = [arg for arg in req.callback_args
                         if not isinstance(arg, SpecDict)] \
            if req.callback_args else ()
        req.callback_args = tuple(callback_args)
        db.Link(key, req).save()

    def robot_main(self, robot):
        if robot.forbidden or not robot.alive or not robot.subscribe:
            robot.payload['alive'] = False
            robot.save()
            robot.remove_sched()
            return

        item_subscribe = []
        for tid in robot.subscribe:
            if db.Item.count_by_index("task_id", tid) > 0:
                item_subscribe.append(tid)

        if item_subscribe:
            yield from self.process_on_item(robot, item_subscribe)
            return

        subscribe = []
        for tid in robot.subscribe:
            if db.Task(tid).link_count() > 0:
                subscribe.append(tid)
        if not subscribe:
            robot.payload['alive'] = False
            robot.remove_sched()
            robot.save()
            return

        yield from self.process_on_task(robot, subscribe)

    def process_on_task(self, robot, subscribe):
        robot_succeed_count = robot.succeed_count
        start_time = time()
        stop_time = start_time + 300
        none_count = 0
        none_limit = 10

        while True:
            yield from asyncio.sleep(1)
            if time() > stop_time or none_count > none_limit:
                break

            task_id = random.choice(subscribe)

            task = db.Task(task_id)

            hash_url = task.link_pop()
            if not hash_url:
                none_count += 1
                task.link_drop()
                continue

            link = db.Link(hash_url)
            req = link.req
            if not req:
                none_count += 1
                continue

            callback_args = list(req.callback_args)
            callback_args.append(SpecDict({
                'robot_id': robot.index,
                'task_id': int(req.group),
            }))
            req.callback_args = tuple(callback_args)

            try:
                yield from self.submit_req(req)
                link.delete()
            except RobotError:
                task.link_push(hash_url)
                break
            except RetryRequest:
                task.link_push(hash_url)
            except Exception as e:
                logger.exception(e)
                task.link_push(hash_url)

            if robot.succeed_count > robot_succeed_count or robot.one_by_one is True:
                break

    def process_on_item(self, robot, subscribe):
        robot_succeed_count = robot.succeed_count
        start_time = time()
        stop_time = start_time + 100
        none_count = 0
        none_limit = 10

        while True:
            yield from asyncio.sleep(1)
            if time() > stop_time or none_count > none_limit:
                break

            if not subscribe:
                break

            task_id = random.choice(subscribe)
            with (yield from self._item_lock):

                item_id = db.Item.pop_queue(task_id)

                if not item_id:
                    db.Item.drop_queue(task_id)
                    subscribe.remove(task_id)
                    if not subscribe:
                        break
                    continue

                item = db.Item(item_id)
                if not item.payload:
                    subscribe.remove(task_id)
                    item.payload['user_id'] = robot.user_id
                    item.payload['task_id'] = task_id
                    item.delete()
                    continue

                pin_item = import_module(item.cls_name, item.payload)
                pin_item["robot_id"] = robot.index
                item.delete()

            try:
                yield from self.push_item(pin_item, True)
            except Exception as e:
                logger.exception(e)

            if robot.succeed_count > robot_succeed_count:
                break

    def push_item(self, item, force_submit=False):
        try:
            robot = db.Robot(item['robot_id'])
            if force_submit or robot.one_by_one is not True:
                yield from self.submit_item(item)
            else:
                key = hash_url(item.imgurl)
                data = item.copy()
                data['hash_url'] = key
                data['cls_name'] = get_cls_name(item)
                ite = db.Item(None, data)
                ite.save()
        except RobotError as e:
            raise e
        except Exception as e:
            logger.exception(e)


class RobotBasedScheduler(RobotBased, UniqScheduler):
    def __init__(self, tasks=4, loop=None):
        RobotBased.__init__(self, tasks * 2)
        UniqScheduler.__init__(self, tasks=tasks, loop=loop)
        self.loop.add_signal_handler(signal.SIGINT, self.signal_handler)
        self.loop.add_signal_handler(signal.SIGTERM, self.signal_handler)
