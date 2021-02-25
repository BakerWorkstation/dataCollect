#!/usr/bin/env python3.6
# -*- coding:utf-8 -*-
# __author__: sdc

import os
import time
import logging
import datetime
from logging import handlers


def record(**kwargs):
    filename = os.path.join('/tmp/', kwargs['filename'])
    level = kwargs['level']
    datefmt = kwargs.pop('datefmt', None)
    format = kwargs.pop('format', None)
    if level is None:
        level = logging.INFO
    if datefmt is None:
        datefmt = '%Y-%m-%d %H:%M:%S'
    if format is None:
        format = '%(asctime)s [%(module)s] %(levelname)s [%(lineno)d] %(message)s'
    log = logging.getLogger(filename)
    format_str = logging.Formatter(format, datefmt)
    th = handlers.TimedRotatingFileHandler(filename=filename, backupCount=7, when='midnight', atTime=datetime.time(0, 0, 0, 0), encoding='utf-8')
    #th._namer = lambda x: 'test.' + x.split[-1]
    th.setFormatter(format_str)
    th.setLevel(level)
    log.addHandler(th)
    log.setLevel(level)
    return log