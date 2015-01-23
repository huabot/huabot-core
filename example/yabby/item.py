from grapy.core import Item

__all__ = ['TweetItem']


class TweetItem(Item):

    _fields = [
        {'name': 'key',      'type': 'str'},
        {'name': 'link',     'type': 'str'},
        {'name': 'imgurl',   'type': 'str'},
        {'name': 'text',     'type': 'str'},
        {'name': 'robot_id', 'type': 'int'},
        {'name': 'task_id',  'type': 'int'},
    ]
