import os
from aio_periodic import Client


if not os.environ.get('PERIODIC_PORT'):
    os.environ['PERIODIC_PORT'] = "unix:///tmp/periodic.sock"


def submit_job(job):
    client = Client()
    client.add_server(os.environ['PERIODIC_PORT'])
    yield from client.connect()
    ret = yield from client.submitJob(job)
    client.close()
    return ret


def remove_job(func, name):
    data = {
        "name": name,
        "func": func
    }
    client = Client()
    client.add_server(os.environ['PERIODIC_PORT'])
    yield from client.connect()
    ret = yield from client.removeJob(data)
    client.close()
    return ret


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
    client = Client()
    client.add_server(os.environ['PERIODIC_PORT'])
    yield from client.connect()
    ret = yield from client.status()
    client.close()

    if funcName:
        return ret.get(funcName, {})

    return {}


def robot_status():
    return (yield from status('process_robot'))


def task_status():
    return (yield from status('update_task'))
