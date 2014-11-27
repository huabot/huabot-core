import os
import os.path
from bottle import request, response

__all__ = [
    "request", "response", "server", "app"
]

from aiobottle import AsyncBottle
app = AsyncBottle()
from . import bottle_login

login_plugin = bottle_login.Plugin()
app.install(login_plugin)

from . import route

REDIS_PORT = os.environ.get('REDIS_PORT', 'tcp://127.0.0.1:6379')

import beaker.session
from beaker.middleware import SessionMiddleware
from .redis_store import RedisManager

beaker.session.clsmap._clsmap["ext:redis"] = RedisManager
session_opts = {
    'session.type': 'ext:redis',
    'session.cookie_expires': 7 * 24 * 60 * 60,
    'session.url': REDIS_PORT[6:],
    'session.auto': True
}

server = SessionMiddleware(app, session_opts)
