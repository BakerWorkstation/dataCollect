'''
@Author: sdc
@Date: 2019-12-31 10:01:23
@LastEditTime : 2019-12-31 10:01:59
@LastEditors  : Please set LastEditors
@Description: In User Settings Edit
@FilePath: /opt/DataCollect/syslog/Config.py
'''
#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
# __author__: sdc

import os
import sys
import redis
import logging
import configparser

from utils.log import record

'''
@description:  配置进程中的环境变量
'''
def _init():
    server_dir = "/opt/DataCollect/pta/"
    global _global_dict
    try:
        conf = configparser.ConfigParser()
        fileUrl = os.path.join(server_dir, "conf/server.conf")
        conf.read(fileUrl)
        web_ip = conf.get("SYSLOG", "ip")
        web_port = int(conf.get("SYSLOG", "port"))
        log_level = conf.get("LOG", "level")
        # 设置日志输出级别
        if log_level == "info":
            log_level = logging.INFO
        elif log_level == "error":
            log_level = logging.ERROR
        elif log_level == "warning":
            log_level = logging.WARNING
        else:
            log_level = logging.ERROR
        logger = record(filename='syslog_server.log', level=log_level)
        _global_dict = {
                        "web_ip": web_ip,
                        "web_port": web_port,
                        "logger": logger
        }
    except Exception as e:
        print(str(e))
        sys.exit()

'''
@description:  返回进程中的环境变量
@return: defValue(dict)
'''
def get_value():
    try:
        return _global_dict
    except KeyError:
        return defValue
