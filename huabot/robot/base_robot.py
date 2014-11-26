import asyncio
from .. import db


class RobotError(Exception):
    pass


class BaseRobot(object):

    def __init__(self, robot_id, robot_class=db.Robot, loop=None):
        self._robot = robot_class(robot_id)
        self.loop = loop
        if not self.loop:
            self.loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def activate(self):
        return True

    @asyncio.coroutine
    def process(self, item):
        raise NotImplementedError('you must rewrite at sub class')

    def set_success(self, item):
        self._robot.incr_succeed_count()
        db.Task(item['task_id']).incr_succeed_count()
        db.User(self._robot.user_id).incr_succeed_count()

    def set_error(self, item):
        pass
