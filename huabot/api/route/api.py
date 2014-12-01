from huabot.api import app, request, response
from huabot.utils import hash_url, submit_task
from huabot.periodic import sched_task, sched_job
from huabot import db
import json
import aiohttp
from datetime import datetime, timedelta


def json_response(key=None, data=None, err=None):
    response.set_header('content-type', 'application/json')
    if data and (isinstance(data, dict) and data.get("err")):
        err = data['err']

    if err:
        return json.dumps({'err': err})
    if key:
        return json.dumps({key: data})

    return json.dumps(data)


@app.post('/api/auth')
def auth():
    username = request.forms.username.strip()
    passwd = request.forms.passwd.strip()

    user = User.get_by_name(username)

    if user:
        if user.passwd == passwd:
            app.login(user.user_id)

        return json_response('result', {"msg": "success"})

    else:
        return json_response(err='Invalid Username or Password')


@app.post('/api/robots/')
def create_robot(user):
    name = request.forms.name
    passwd = request.forms.passwd
    subscribe = request.forms.subscribe
    day_limit = request.forms.day_limit
    one_by_one = request.forms.one_by_one
    extra = request.forms.extra

    if subscribe:
        subscribe = [int(s) for s in subscribe.split(',') if s.strip()]
    else:
        subscribe = []

    one_by_one = False
    if one_by_one and one_by_one == 'true':
        one_by_one = True

    if day_limit:
        day_limit = int(day_limit)

    info = {
        'name': name,
        'passwd': passwd,
        'subscribe': subscribe,
        'alive': True,
        'day_limit': day_limit,
        'user_id': user.user_id,
        'one_by_one': one_by_one,
    }

    if extra:
        extra = json.loads(extra)
        info.update(extra)

    if db.Robot.get_by_name(name):
        return json_response(err='robot %s is already added.' % name)

    robot = db.Robot(None, info)
    robot.save()
    robot.sched_now()
    yield from sched_job(robot)
    for task_id in subscribe:
        db.Task(task_id).subscribed_new(robot.index)
        submit_task(db.Task(task_id))

    return json_response('robot', robot.payload.copy())


@app.post('/api/robots/<robot_id:re:\d+>')
def update_robot(robot_id, user):
    robot_id = int(robot_id)
    name = request.forms.name
    passwd = request.forms.passwd
    day_limit = request.forms.day_limit
    subscribe = request.forms.subscribe
    one_by_one = request.forms.one_by_one

    if subscribe:
        subscribe = [int(s) for s in subscribe.split(',') if s.strip()]
    else:
        subscribe = []

    one_by_one = False
    if one_by_one and one_by_one == 'true':
        one_by_one = True

    if day_limit:
        day_limit = int(day_limit)

    info = {
        'name': name,
        'passwd': passwd,
        'forbidden': False,
        'alive': True,
        'day_limit': day_limit,
        'subscribe': subscribe,
        'one_by_one': one_by_one,
    }

    if extra:
        extra = json.loads(extra)
        info.update(extra)

    old_robot = db.Robot.get(robot_id)
    if not old_robot:
        return json_response(err='robot %s not found' % robot_id)

    if old_robot.user_id != user.user_id:
        return json_response(err='no permission')

    if old_robot.name != info['name']:
        if db.Robot.get_by_name(name):
            return json_response(err='robot %s is already added.' % name)

    robot = old_robot.payload.copy()
    robot.update(info)
    robot = db.Robot(robot_id, robot)
    robot.save()
    robot.sched_now()
    yield from sched_job(robot)

    old = []
    old_subscribe = old_robot.subscribe or []
    for task_id in old_subscribe:
        if task_id not in subscribe:
            old.append(task_id)

    for task_id in subscribe:
        db.Task(task_id).subscribed_new(robot.index)
        submit_task(db.Task(task_id))

    for task_id in old:
        db.Task(task_id).subscribed_rm(robot.index)

    return json_response('robot', robot.payload.copy())


@app.delete('/api/robots/<robot_id:re:\d+>')
def remove_robot(robot_id, user):
    robot_id = int(robot_id)
    robot = db.Robot(robot_id)
    if robot.user_id != user.user_id:
        return json_response(err='no permission')

    robot.delete()

    return '{}'


@app.post('/api/robots/<robot_id:re:\d+>/start')
def start_robot(robot_id, user):
    robot_id = int(robot_id)
    robot = db.Robot(robot_id)
    if robot.user_id != user.user_id:
        return json_response(err='no permission')
    robot.payload['forbidden'] = False
    robot.payload['alive'] = True

    robot.save()
    robot.sched_now()
    yield from sched_job(robot)

    return json_response('robot', robot.payload.copy())


@app.post('/api/robots/<robot_id:re:\d+>/stop')
def stop_robot(robot_id, user):
    robot_id = int(robot_id)
    robot = db.Robot(robot_id)
    if robot.user_id != user.user_id:
        return json_response(err='no permission')
    robot.payload['alive'] = False

    robot.save()
    robot.remove_sched()

    return json_response('robot', robot.payload.copy())


@app.post('/api/tasks/')
def create_task(user):
    url = request.forms.url
    desc = request.forms.desc
    spider = request.forms.spider
    proxies = request.forms.proxies
    refresh_delay = request.forms.refresh_delay
    extra = request.forms.extra

    if proxies:
        proxies = proxies.split(",")

    else:
        proxies = []

    if refresh_delay:
        refresh_delay = int(refresh_delay)

    info = {
        'url': url,
        'desc': desc,
        'spider': spider,
        'proxies': proxies,
        'user_id': user.user_id,
        'refresh_delay': refresh_delay,
    }
    info['hash_url'] = hash_url(url)
    if db.Task.get_by_hash_url(info['hash_url']):
        return json_response(err='task %s is already added' % url)

    if extra:
        extra = json.loads(extra)
        info.update(extra)

    task = db.Task(None, info)
    task.save()
    submit_task(task)
    if task.refresh_delay:
        task.sched_later(task.refresh_delay)
        yield from sched_task(task)

    return json_response('task', info)


@app.post('/api/tasks/<task_id:re:\d+>')
def update_task(task_id, user):
    task_id = int(task_id)
    url = request.forms.url
    desc = request.forms.desc
    spider = request.forms.spider
    proxies = request.forms.proxies
    refresh_delay = request.forms.refresh_delay
    extra = request.forms.extra

    if refresh_delay:
        refresh_delay = int(refresh_delay)

    if proxies:
        proxies = proxies.split(",")

    else:
        proxies = []

    info = {
        'url': url,
        'desc': desc,
        'spider': spider,
        'proxies': proxies,
        'refresh_delay': refresh_delay,
    }
    info['hash_url'] = hash_url(url)

    if extra:
        extra = json.loads(extra)
        info.update(extra)

    old_task = db.Task(task_id)
    if not old_task.payload:
        return json_response(err='task %s not found' % task_id)

    if old_task.user_id != user.user_id:
        return json_response(err='no permission')

    if old_task.url != info['url']:
        if db.Task.get_by_hash_url(info['hash_url']):
            return json_response(err='task %s is already added' % url)

    task = old_task.payload.copy()
    task.update(info)
    task = db.Task(task_id, task)
    task.save()

    submit_task(task)
    if task.refresh_delay:
        task.sched_later(task.refresh_delay)
        yield from sched_task(task)

    return json_response('task', task.payload.copy())


@app.delete('/api/tasks/<task_id:re:\d+>')
def remove_task(task_id, user):
    task_id = int(task_id)
    task = db.Task(task_id)
    if task.user_id != user.user_id:
        return json_response(err='no permission')

    task.delete()

    return '{}'


@app.delete('/api/tasks/<task_id:re:\d+>/clear_uniq')
def remove_task_link(task_id, user):
    task_id = int(task_id)
    task = db.Task(task_id)
    if task.user_id != user.user_id:
        return json_response(err='no permission')

    task.clear_uniq()

    return '{}'


@app.get('/api/tasks/')
def get_tasks(user):
    page = request.query.page
    limit = request.query.limit
    if page:
        page = int(page)
    else:
        page = 1

    if limit:
        limit = int(limit)
    else:
        limit = 20

    if limit > 100:
        limit = 100

    task_count = user.task_count

    start = limit * (page - 1)
    stop = start + limit - 1

    if stop > task_count:
        stop = task_count

    if start > task_count:
        start = task_count - limit

    task_start = task_count - stop
    task_stop = task_count - start

    task_ids = db.Task.range_by_user_id(user.user_id, task_start, task_stop)
    task_ids = sorted(task_ids, key=lambda x: int(x.member), reverse=True)

    tasks = []
    for index in task_ids:
        task = db.Task(index.member)
        if not task.payload:
            continue
        data = task.payload.copy()
        data['succeed_count'] = task.succeed_count
        tasks.append(data)

    return json_response(data={
        'total': task_count,
        'page': page,
        'limit': limit,
        'tasks': tasks
    })


@app.get("/api/<type:re:year|month|day|hour|minute>/succeed_count")
def get_minute_succeed_count(type, user):
    now = datetime.now()
    members = []

    count = 80

    year = now.year
    month = now.month
    for i in range(count):
        if type == "year":
            members.append("%s" % (year - i))
            subfix = "-01-01 00:00:00"
        elif type == "month":
            n_month = month - i
            n_year = year
            if n_month <= 0:
                n_year = n_year - 1
                n_month = 12 + n_month
            members.append("%s-%s" % (n_year, n_month))
            subfix = "-01 00:00:00"
        elif type == "day":
            date = now - timedelta(days=i)
            members.append("%s-%s-%s" % (date.year, date.month, date.day))
            subfix = " 00:00:00"
        elif type == "hour":
            date = now - timedelta(hours=i)
            members.append("%s-%s-%s %s" % (
                date.year, date.month, date.day, date.hour))
            subfix = ":00:00"
        else:
            date = now - timedelta(minutes=i)
            members.append("%s-%s-%s %s:%s" % (
                date.year, date.month, date.day, date.hour, date.minute))
            subfix = ":00"

    retval = []

    for member in members:
        retval.append([member + subfix, user.get_time_succeed_count(member)])

    return json_response('result', retval)


@app.get(
    '/api/robots/<robot_id:re:\d+>/'
    '<type:re:year|month|day|hour|minute>/succeed_count')
def get_robot_hour_succeed_count(robot_id, type, user):
    now = datetime.now()
    members = []
    subfix = ""
    robot_id = int(robot_id)
    robot = db.Robot(robot_id)

    count = 80

    year = now.year
    month = now.month
    for i in range(count):
        if type == "year":
            members.append("%s" % (year - i))
            subfix = "-01-01 00:00:00"
        elif type == "month":
            n_month = month - i
            n_year = year
            if n_month <= 0:
                n_year = n_year - 1
                n_month = 12 + n_month
            members.append("%s-%s" % (n_year, n_month))
            subfix = "-01 00:00:00"
        elif type == "day":
            date = now - timedelta(days=i)
            members.append("%s-%s-%s" % (date.year, date.month, date.day))
            subfix = " 00:00:00"
        elif type == "hour":
            date = now - timedelta(hours=i)
            members.append("%s-%s-%s %s" % (
                date.year, date.month, date.day, date.hour))
            subfix = ":00:00"
        else:
            date = now - timedelta(minutes=i)
            members.append("%s-%s-%s %s:%s" % (
                date.year, date.month, date.day, date.hour, date.minute))
            subfix = ":00"

    retval = []

    for member in members:
        retval.append([member + subfix, robot.get_time_succeed_count(member)])

    return json_response('result', retval)


@app.get(
    '/api/tasks/<task_id:re:\d+>/'
    '<type:re:year|month|day|hour|minute>/succeed_count')
def get_task_hour_succeed_count(task_id, type, user):
    now = datetime.now()
    members = []
    subfix = ""
    task_id = int(task_id)
    task = db.Task(task_id)

    count = 80
    year = now.year
    month = now.month
    for i in range(count):
        if type == "year":
            members.append("%s" % (year - i))
            subfix = "-01-01 00:00:00"
        elif type == "month":
            n_month = month - i
            n_year = year
            if n_month <= 0:
                n_year = n_year - 1
                n_month = 12 + n_month
            members.append("%s-%s" % (n_year, n_month))
            subfix = "-01 00:00:00"
        elif type == "day":
            date = now - timedelta(days=i)
            members.append("%s-%s-%s" % (date.year, date.month, date.day))
            subfix = " 00:00:00"
        elif type == "hour":
            date = now - timedelta(hours=i)
            members.append("%s-%s-%s %s" % (
                date.year, date.month, date.day, date.hour))
            subfix = ":00:00"
        else:
            date = now - timedelta(minutes=i)
            members.append("%s-%s-%s %s:%s" % (
                date.year, date.month, date.day, date.hour, date.minute))
            subfix = ":00"

    retval = []

    for member in members:
        retval.append([member + subfix, task.get_time_succeed_count(member)])

    return json_response('result', retval)
