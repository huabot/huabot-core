import os
import os.path
from bottle import TEMPLATE_PATH, static_file, request, response,\
    template, redirect

__all__ = [
    "static_file", "request", "response", "template", "redirect", "views"
]

TEMPLATE_PATH.insert(0, os.path.join(os.path.dirname(__file__), './templates'))
from aiobottle import AsyncBottle
app = AsyncBottle()
from . import bottle_login

login_plugin = bottle_login.Plugin(redirect_to='/signin')
app.install(login_plugin)


@app.route('/static/<path:re:.+>')
def server_static(path):
    return static_file(path,  root=os.path.join(
        os.path.dirname(__file__), 'static'))


@app.route('/favicon.ico')
def server_favicon():
    return static_file('favicon.ico',  root=os.path.join(
        os.path.dirname(__file__), 'static'))

from . import views

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
