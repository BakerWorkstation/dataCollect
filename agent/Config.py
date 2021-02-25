#!/usr/bin/env python3.6
# -*- coding:UTF-8 -*-
# __author__: sdc

import os
import sys
import logging
import asyncio
import configparser

from log import record

def _init():
    server_dir = "./"
    global _global_dict
    try:
        conf = configparser.ConfigParser()
        fileUrl = os.path.join(server_dir, "server.conf")
        conf.read(fileUrl)
        api_ip = conf.get("API", "ip")
        api_port = int(conf.get("API", "port"))
        cmdb_u = conf.get("AUTH", "username")
        cmdb_p = conf.get("AUTH", "password")
        log_level = conf.get("LOG", "level")
        logfile = conf.get("COLLECT", "file")
        # 设置日志输出级别
        if log_level == "info":
            log_level = logging.INFO
        elif log_level == "error":
            log_level = logging.ERROR
        elif log_level == "warning":
            log_level = logging.WARNING
        else:
            log_level = logging.ERROR

        logger = record(filename='agent_server.log', level=log_level)
        _global_dict = {
                        "api_ip": api_ip,
                        "api_port": api_port,
                        "cmdb_u": cmdb_u,
                        "cmdb_p": cmdb_p,
                        "server_dir": server_dir,
                        "logger": logger,
                        "logfile": logfile
        }
    except Exception as e:
        print(str(e))
        sys.exit()

# def set_value(name, value):
#     _global_dict[name] = value

def get_value(name='score', defValue=None):
    try:
        return _global_dict
    except KeyError:
        return defValue
