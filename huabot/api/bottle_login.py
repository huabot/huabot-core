__author__ = "Li Meng Jun"
__version__ = '0.0.1'
__license__ = 'MIT'

# CUT HERE (see setup.py)

import inspect
from bottle import request, redirect, PluginError, response
from huabot import db


class LoginPlugin(object):
    '''
    '''
    name = 'login'

    __slots__ = ['session', 'app', 'redirect_to', 'session_key', 'keyword']

    def __init__(self, session_key='user', keyword='user', redirect_to=None):
        self.keyword = keyword
        self.session_key = session_key
        self.redirect_to = redirect_to
        self.app = None
        self.session = {}

    def setup(self, app):
        '''
        Make sure that other installed plugins don't affect the same keyword
        argument.
        '''
        self.app = app
        for other in app.plugins:
            if not isinstance(other, LoginPlugin):
                continue
            if other.keyword == self.keyword:
                raise PluginError(
                    "Found another login plugin with conflicting settings"
                    "(non-unique keyword).")
        self.app.add_hook('before_request', self.load_session)
        self.app.add_hook('after_request', self.set_session)
        self.app.login = self.login
        self.app.logout = self.logout

    def load_session(self):
        self.session = request.environ.get('beaker.session', {})

    def set_session(self):
        pass

    def login(self, user):
        '''
        Store the login user to session.
        '''
        self.session[self.session_key] = user

    def logout(self):
        self.session[self.session_key] = None

    def apply(self, callback, context):
        conf = context['config'].get('login') or {}
        keyword = conf.get('keyword', self.keyword)
        session_key = conf.get('session_key', self.session_key)
        redirect_to = conf.get('redirect_to', self.redirect_to)
        args = inspect.getargspec(context['callback'])[0]
        request.user = None
        user_id = self.session.get(session_key)
        if user_id:
            request.user = db.User.get(user_id)
        if keyword not in args:
            return callback

        def wrapper(*args, **kwargs):
            user_id = self.session.get(session_key)
            if user_id:
                kwargs[keyword] = db.User.get(user_id)
                return callback(*args, **kwargs)
            if redirect_to:
                return redirect(redirect_to)
            response.status = 403
            return {'err': 'not permission'}

        # Replace the route callback with the wrapped one.
        return wrapper

Plugin = LoginPlugin
