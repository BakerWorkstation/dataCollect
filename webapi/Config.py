'''
@Author: your name
@Date: 2020-05-29 11:35:56
@LastEditTime: 2020-05-29 11:36:26
@LastEditors: Please set LastEditors
@Description: In User Settings Edit
@FilePath: /opt/DataCollect/webapi/Config.py
'''
#!/usr/bin/env python3.6
# -*- coding:UTF-8 -*-
# __author__: sdc

import os
import sys
import json
import redis
import logging
import asyncio
import configparser
import asyncio_redis

from log import record

def _init():
    server_dir = "/opt/DataCollect/webapi/"
    global _global_dict
    try:
        global_conf = configparser.ConfigParser()
        global_conf.read("/opt/DataCollect/global.conf")
        conf = configparser.ConfigParser()
        fileUrl = os.path.join(server_dir, "conf/server.conf")
        conf.read(fileUrl)
        web_ip = conf.get("WEB", "ip")
        web_port = int(conf.get("WEB", "port"))
        web_process = int(conf.get("WEB", "process"))
        redis_ip = global_conf.get("REDIS", "ip")
        redis_port = global_conf.get("REDIS", "port")
        redis_db = conf.get("REDIS", "db")
        redis_passwd = global_conf.get("REDIS", "password")
        redis_expire = conf.get("REDIS", "expire")
        kafka_ip = global_conf.get("KAFKA", "ip")
        kafka_port = int(global_conf.get("KAFKA", "port"))
        origin_api = conf.get("KAFKA", "origin_api")
        hbase_ip = global_conf.get("HBASE", "ip")
        hbase_port = int(global_conf.get("HBASE", "port"))
        hbase_tube = conf.get("HBASE", "tube")
        cmdb_u = conf.get("CMDB", "username")
        cmdb_p = conf.get("CMDB", "password")
        pg_conn = json.loads(global_conf.get("PG", "pg_conn"))
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
            host="10.255.175.96",
            port=redis_port,
            db=redis_db,
            password="antiy?pmc",
            decode_responses=True
        )
        redis_pool = redis.Redis(connection_pool=pool)
        logger = record(filename='api_server.log', level=log_level)
        _global_dict = {
                        "web_ip": web_ip,
                        "web_port": web_port,
                        "web_process": web_process,
                        "redis_pool": redis_pool,
                        "redis_expire": redis_expire,
                        "kafka_ip": kafka_ip,
                        "kafka_port": kafka_port,
                        "origin_api": origin_api,
                        "hbase_ip": hbase_ip,
                        "hbase_port": hbase_port,
                        "hbase_tube": hbase_tube,
                        "cmdb_u": cmdb_u,
                        "cmdb_p": cmdb_p,
                        "server_dir": server_dir,
                        "pg_conn": pg_conn,
                        "logger": logger
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
