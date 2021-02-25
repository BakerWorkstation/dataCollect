'''
@Author: sdc
@Date: 2019-12-23 14:45:04
@LastEditTime: 2020-05-29 14:01:12
@LastEditors: Please set LastEditors
@Description: In User Settings Edit
@FilePath: /opt/DataCollect/iep_asset_sync/Config.py
'''

#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
# __author__: sdc

import os
import sys
import json
import redis
import logging
import configparser

from utils.log import record


'''
@description:  配置进程中的环境变量
'''
def _init():
    server_dir = "/opt/DataCollect/iep_asset_sync/"
    global _global_dict
    try:
        global_conf = configparser.ConfigParser()
        global_conf.read("/opt/DataCollect/global.conf")
        conf = configparser.ConfigParser()
        fileUrl = os.path.join(server_dir, "conf/server.conf")
        conf.read(fileUrl)
        rule_conf = configparser.ConfigParser()
        ruleUrl = os.path.join(server_dir, "conf/rules.conf")
        rule_conf.read(ruleUrl)
        pg_conn = json.loads(global_conf.get("PG", "pg_conn"))
        kafka_ip = global_conf.get("KAFKA", "ip")
        kafka_port = int(global_conf.get("KAFKA", "port"))
        partitions = int(global_conf.get("KAFKA", "partitions"))
        group_id = conf.get("KAFKA", "group_id")
        session_timeout = int(conf.get("KAFKA", "session_timeout"))
        reset = conf.get("KAFKA", "reset")
        max_poll = conf.get("KAFKA", "max_poll")
        length = int(conf.get("KAFKA", "length"))
        redis_ip = global_conf.get("REDIS", "ip")
        redis_port = global_conf.get("REDIS", "port")
        redis_passwd = global_conf.get("REDIS", "password")
        redis_db = conf.get("REDIS", "db")
        log_level = conf.get("LOG", "level")
        ip = conf.get("IEP", "ip").strip()
        if ip == "-1":
            limit_device = ip
        else:
            limit_device = list(map(lambda x: x.strip(), ip.split(",")))
        _global_dict = {
                        "pg_host": pg_conn["host"],
                        "pg_port": pg_conn["port"],
                        "pg_db": pg_conn["database"],
                        "pg_user": pg_conn["user"],
                        "pg_passwd": pg_conn["password"],
                        "pg_conn": pg_conn,
                        "kafka_ip": kafka_ip,
                        "kafka_port": kafka_port,
                        "kafka_group_id": group_id,
                        "kafka_session_timeout": session_timeout,
                        "kafka_reset": reset,
                        "kafka_max_poll": max_poll,
                        "kafka_partitions": partitions,
                        "length": length,
                        "redis_ip": redis_ip,
                        "redis_port": redis_port,
                        "redis_db": redis_db,
                        "redis_passwd": redis_passwd,
                        "limit_device": limit_device
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
