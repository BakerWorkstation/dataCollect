'''
@Author: sdc
@Date: 2019-12-31 10:01:23
@LastEditTime: 2020-06-04 16:10:44
@LastEditors: Please set LastEditors
@Description: In User Settings Edit
@FilePath: /opt/DataCollect/syslog/Config.py
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
    server_dir = "/opt/DataCollect/syslog/"
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
        web_ip = conf.get("SYSLOG", "ip")
        web_port = int(conf.get("SYSLOG", "port"))
        process = int(conf.get("SYSLOG", "process"))
        kafka_ip = global_conf.get("KAFKA", "ip")
        group_id = conf.get("KAFKA", "group_id")
        session_timeout = int(conf.get("KAFKA", "session_timeout"))
        kafka_port = int(global_conf.get("KAFKA", "port"))
        origin_syslog = conf.get("KAFKA", "origin_syslog")
        handle_syslog = conf.get("KAFKA", "handle_syslog")
        reset = conf.get("KAFKA", "reset")
        max_poll = conf.get("KAFKA", "max_poll")
        length = int(conf.get("KAFKA", "length"))
        partitions = int(global_conf.get("KAFKA", "partitions"))
        pg_conn = json.loads(global_conf.get("PG", "pg_conn"))
        hbase_ip = global_conf.get("HBASE", "ip")
        hbase_port = int(global_conf.get("HBASE", "port"))
        hbase_tube = conf.get("HBASE", "table")
        redis_ip = global_conf.get("REDIS", "ip")
        redis_port = global_conf.get("REDIS", "port")
        redis_passwd = global_conf.get("REDIS", "password")
        redis_db = conf.get("REDIS", "db")
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
        # 开启redis_pool 连接池
        pool = redis.ConnectionPool(
            host=redis_ip,
            port=redis_port,
            db=redis_db,
            password=redis_passwd,
            decode_responses=True
        )
        redis_pool = redis.Redis(connection_pool=pool)
        logger = record(filename='syslog_server.log', level=log_level)
        _global_dict = {
                        "web_ip": web_ip,
                        "web_port": web_port,
                        "process": process,
                        "kafka_ip": kafka_ip,
                        "kafka_port": kafka_port,
                        "group_id": group_id,
                        "origin_syslog": origin_syslog,
                        "handle_syslog": handle_syslog,
                        "pg_conn": pg_conn,
                        "partitions": partitions,
                        "kafka_reset": reset,
                        "kafka_max_poll": max_poll,
                        "length": length,
                        "session_timeout": session_timeout,
                        "hbase_ip": hbase_ip,
                        "hbase_port": hbase_port,
                        "hbase_tube": hbase_tube,
                        "server_dir": server_dir,
                        "redis_pool": redis_pool,
                        "logger": logger,
                        "rule_conf": rule_conf
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
