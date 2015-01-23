import asyncio
import aiohttp
from time import time

import os
from huabot.utils import logger, json_decode, hash_url
from huabot.robot import BaseRobot, RobotError

YB_HOST = os.environ.get('YB_HOST', 'huabot.com')

class YabbyRobot(BaseRobot):

    def __init__(self, robot_id, loop=None):
        BaseRobot.__init__(self, robot_id, loop = loop)

        self.host = 'http://' + YB_HOST

        self.is_login = False

        self.connector = aiohttp.TCPConnector(share_cookies=True,
                                              loop=self.loop, conn_timeout=60)

        self.headers = {}
        self.headers['User-Agent'] = 'Huabot'
        self.headers['Accept'] = 'application/json'

        self.token = {
            'access_token': self._robot.access_token,
            'expires_at': self._robot.expires_at,
            'refresh_token': self._robot.refresh_token,
            'expires_in': self._robot.expires_in
        }

        if self.token['access_token']:
            self.headers['Authorization'] = 'Bearer ' + self.token['access_token']

        self.user = {'username': self._robot.username}

    @asyncio.coroutine
    def activate(self):
        is_login = yield from self.test_auth()
        if not is_login:
            if self.token['refresh_token']:
                yield from self.refresh_token()

            if not self.is_login:
                yield from self.auth()

        return self.is_login

    def test_auth(self):
        ret = yield from self._request('post', '/users/me')

        if ret.get('err') or ret.get('code', 0) == 404:
            logger.error(ret)
            return False
        else:
            self.user = ret['user']
            self.update_robot()
            self.is_login = True
            return True

    def refresh_token(self):
        logger.info(
            "Try to login into huabot: %s" % self._robot.payload['email'])

        token = yield from self._request('post', '/auth/', data={
            'type': 'refresh_token'})

        if token.get('err'):
            logger.error("Refresh token failed: %s:%s" % (
                self._robot.payload['email'],
                self._robot.payload['passwd']))
            logger.error("Error: %s" % token)
            raise RobotError()
        else:
            self.is_login = True
            self.token = token
            self.headers['Authorization'] = 'Bearer ' + self.token['access_token']
            self.token['expires_at'] = time() + token['expires_in']
            self.update_robot()

    def auth(self):
        logger.info(
            "Try to login into huabot: %s" % self._robot.payload['email'])
        token = yield from self._request('post', '/auth/', data={
                'email': self._robot.payload['email'],
                'password': self._robot.payload['passwd'],
                'type': 'access_token'
            })

        if token.get('err'):
            logger.error("Login failed: %s:%s" % (
                self._robot.payload['email'],
                self._robot.payload['passwd']))
            logger.error("Error: %s" % token)
            raise RobotError()
        else:
            self.is_login = True
            self.token = token
            self.headers['Authorization'] = 'Bearer ' + self.token['access_token']
            self.token['expires_at'] = time() + token['expires_in']
            self.update_robot()

    def update_robot(self):
        change = False
        keys = ['expires_in', 'access_token', 'refresh_token', 'expires_at']
        for key in keys:
            if self._robot.payload[key] != self.token[key]:
                self._robot.payload[key] = self.token[key]
                change = True

        keys = ['username']
        for key in keys:
            if self._robot.payload[key] != self.user[key]:
                self._robot.payload[key] = self.user[key]
                change = True

        if change:
            self._robot.save()

    def process(self, tweet):
        if self._robot.has_item(hash_url(tweet['text'])):
            return 0

        tweet['text'] = tweet['text'][:150]

        data = {'text': tweet['text']}
        if tweet.get('img_url'):
            data['img_url'] = tweet['img_url']

        retval = yield from self._request('post', '/tweets/', data)

        if retval.get('err'):
            logger.error("User {} post tweet Error: {}".format(
                self.user['username'], retval['msg']))
            return -1

        self.set_success(tweet)
        return retval['tweet']['tweet_id']

    def _request(self, method, uri, data=None):
        rsp = yield from aiohttp.request(method, self.host + uri, data=data,
                                         headers=self.headers,
                                         connector=self.connector)
        ret = yield from rsp.read()
        return json_decode(ret)
