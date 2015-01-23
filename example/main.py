#!/usr/bin/env python3
import argparse
from grapy import engine
from huabot import sched
from grapy.utils import (
    import_middlewares,
    import_pipelines,
    import_spiders,
    logger as grapy_logger
)

from config import spiders, pipelines, middlewares
import os
import logging
import logging.handlers
from huabot.utils import logger


def start_engine(flag=True):
    engine.set_spiders(import_spiders(spiders))
    engine.set_pipelines(import_pipelines(pipelines))
    engine.set_middlewares(import_middlewares(middlewares))
    engine.headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4)'
        ' AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153',
    }
    engine.start(flag)


def start_web():
    from bottle import run
    import www
    from huabot.api import server
    host = os.environ.get("HOST", 'localhost')
    port = os.environ.get("PORT", "8080")
    run(server, host=host, port=port, server='aiobottle:AsyncServer')


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--debug', action='store_true')
    parser.add_argument('-l', '--log', help='the output log file')
    parser.add_argument(
        '-t', '--tasks', type=int, default=5, help='the tasks default: 5')
    parser.add_argument('-H',
                        "--entrypoint",
                        default=os.environ.get("PERIODIC_PORT",
                                               "unix:///tmp/periodic.sock"),
                        help='the periodic task system server address." + \
                        " default: ' + os.environ.get(
                            "PERIODIC_PORT", "unix:///tmp/periodic.sock"))

    parser.add_argument("cmd", help="the command: engine, web, all.")

    args = parser.parse_args()
    return args


def main():
    args = parse_args()

    FORMAT = '%(asctime)-15s - %(message)s'
    formater = logging.Formatter(FORMAT)
    log = args.log
    if log:
        ch = logging.handlers.TimedRotatingFileHandler(
            log, encoding="utf-8", when="d", backupCount=10)
    else:
        ch = logging.StreamHandler()

    ch.setFormatter(formater)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(ch)
    if args.debug:
        grapy_logger.setLevel(logging.DEBUG)
    else:
        grapy_logger.setLevel(logging.INFO)

    os.environ["PERIODIC_PORT"] = args.entrypoint

    grapy_logger.addHandler(ch)

    engine.set_sched(sched.RobotBasedScheduler(tasks=args.tasks))

    if args.cmd == 'engine':
        start_engine(True)

    elif args.cmd == 'web':
        start_web()

    else:
        start_engine(False)
        start_web()


if __name__ == '__main__':
    main()
