'''
@Author: sdc
@Date: 2019-12-18 11:01:08
@LastEditTime : 2019-12-30 16:59:04
@LastEditors  : sdc
@Description: log record
@FilePath: /opt/DataCollect/iep_asset_sync/utils/log.py
'''

#!/usr/bin/env python3.6
# -*- coding:utf-8 -*-
# __author__: sdc

import os
import time
import logging
import datetime
from logging import handlers

'''
@description:  创建日志句柄公共函数
@param    kwargs(dict)
@return:  log(object)
'''
def record(**kwargs):
    dirpath = '/var/log/datacollect/IEP-ACD/'
    process = kwargs.pop('process', None)
    thread = kwargs.pop('thread', None)
    if process:
        dirpath = os.path.join(dirpath, process)
    if thread or thread == 0:
        dirpath = os.path.join(dirpath, ('thread-%s' % thread))
    try:
        os.makedirs(dirpath)
    except:
        pass
    filename = os.path.join(dirpath, kwargs['filename'])
    level = None#kwargs['level']
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
    th = handlers.TimedRotatingFileHandler(filename=filename, backupCount=7, when='midnight', encoding='utf-8')
    #th._namer = lambda x: 'test.' + x.split[-1]
    th.setFormatter(format_str)
    th.setLevel(level)
    log.addHandler(th)
    log.setLevel(level)
    return log