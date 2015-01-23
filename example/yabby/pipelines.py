from .item import TweetItem
from .robot import YabbyRobot
from huabot.utils import hash_url, get_cls_name
from huabot import db
from grapy.core.exceptions import DropItem


class RobotPipeline(object):
    def process(self, item):
        if isinstance(item, TweetItem):
            robot = YabbyRobot(item.robot_id)
            ret_id = 0
            key = hash_url(item.text)
            data = item.copy()
            data['hash'] = key
            data['cls_name'] = get_cls_name(item)
            ite = db.Item(None, data)
            ite.incr_visit()
            try:
                is_activate = yield from robot.activate()
                if is_activate:
                    ret_id = yield from robot.process(item)
            except DropItem:
                ret_id = 1
            finally:
                if not ite.try_count:
                    ite.payload['try_count'] = 0
                ite.payload['try_count'] += 1
                if ret_id == 0 and ite.try_count < 10:
                    try:
                        ite.save()
                    except Exception:
                        pass

        return item
