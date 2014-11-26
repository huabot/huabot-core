import aiohttp


def submit_job(job):
    rsp = yield from aiohttp.request("POST", os.environ['PERIODIC_PORT'], 
                                     data = job)

    ret = yield from rsp.read()

    return json_decode(ret)


def remove_job(func, name):
    data = {
        "name": name,
        "func": func,
        "act": "remove"
    }
    rsp = yield from aiohttp.request("POST", os.environ['PERIODIC_PORT'], 
                                     data = data)

    ret = yield from rsp.read()

    return json_decode(ret)


def sched_robot(robot):
    data = {
        'name': str(robot.index),
        'func': 'process_robot',
        'sched_at': int(robot.sched_at),
        'timeout': 500
    }

    return (yield from submit_job(data))


def remove_robot(robot):
    return (yield from remove_job('process_robot', int(robot.index)))


def sched_task(task):
    data = {
        'name': str(task.index),
        'func': 'update_task',
        'sched_at': int(task.sched_at),
        'timeout': 500
    }

    return (yield from submit_job(data))


def remove_task(task):
    return (yield from remove_job('update_task', int(task.index)))


def status(funcName=""):
    rsp = yield from aiohttp.request(
        "GET", os.environ['PERIODIC_PORT'] + '/' + funcName)

    ret = yield from rsp.read()
    return json_decode(ret)


def robot_status():
    return (yield from status('process_robot'))


def task_status():
    return (yield from status('update_task'))
