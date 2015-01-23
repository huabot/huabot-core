import os
import os.path
from bottle import TEMPLATE_PATH, static_file, request, response,\
    template, redirect

from huabot.api import app

__all__ = [
    "static_file", "request", "response", "template", "redirect", "views"
]

TEMPLATE_PATH.insert(0, os.path.join(os.path.dirname(__file__), './templates'))

@app.route('/static/<path:re:.+>')
def server_static(path):
    return static_file(path,  root=os.path.join(
        os.path.dirname(__file__), 'static'))


@app.route('/favicon.ico')
def server_favicon():
    return static_file('favicon.ico',  root=os.path.join(
        os.path.dirname(__file__), 'static'))

from . import views
