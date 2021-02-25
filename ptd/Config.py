#!/usr/bin/env python3.6
# -*- coding:UTF-8 -*-
# __author__: sdc

import os
import sys
import redis
import logging
import configparser

from log import record

def _init():
    server_dir = "/opt/DataCollect/ptd/"
    global _global_dict
    try:
        global_conf = configparser.ConfigParser()
        global_conf.read("/opt/DataCollect/global.conf")
        conf = configparser.ConfigParser()
        fileUrl = os.path.join(server_dir, "conf/server.conf")
        conf.read(fileUrl)
        pg_conn = global_conf.get("PG", "pg_conn")
        hbase_ip = global_conf.get("HBASE", "ip")
        hbase_port = int(global_conf.get("HBASE", "port"))
        kafka_ip = global_conf.get("KAFKA", "ip")
        kafka_port = int(global_conf.get("KAFKA", "port"))
        redis_ip = global_conf.get("REDIS", "ip")
        redis_port = int(global_conf.get("REDIS", "port"))
        redis_passwd = global_conf.get("REDIS", "password")
        redis_db = conf.get("REDIS", "db")
        reset = conf.get("kafka", "reset")
        group_id = conf.get("kafka", "group_id")
        topic = conf.get("kafka", "topic")
        asset_topic = conf.get("kafka", "asset_topic")
        log_level = conf.get("LOG", "level")
        asset_reg = global_conf.get("asset_reg","asseturl")
        pro_num = conf.get("process_amount", "pro_num")
        access_dev = conf.get("access_dev", "dev_list")
        # 设置日志输出级别
        if log_level == "info":
            log_level = logging.INFO
        elif log_level == "error":
            log_level = logging.ERROR
        elif log_level == "warning":
            log_level = logging.WARNING
        else:
            log_level = logging.ERROR

        logger = record(filename='ptd_server.log', level=log_level)
        logger_assets = record(filename='make_assets.log', level=log_level)
        logger_kafka = record(filename='kafka_operating.log', level=log_level)
        # 开启redis_pool 连接池
        pool = redis.ConnectionPool(
        host=redis_ip,
        port=redis_port,
        db=redis_db,
        password=redis_passwd,
        decode_responses=True
        )
        redis_pool = redis.Redis(connection_pool=pool)
        _global_dict = {
                        "server_dir": server_dir,
                        "pg_conn": pg_conn,
                        "kafka_ip": kafka_ip,
                        "kafka_port": kafka_port,
                        "reset": reset,
                        "group_id": group_id,
                        "topic": topic,
                        "asset_topic": asset_topic,
                        "hbase_ip": hbase_ip,
                        "hbase_port": hbase_port,
                        "logger": logger,
                        "logger_assets":logger_assets,
                        "logger_kafka":logger_kafka,
                        "asset_reg": asset_reg,
                        "pro_num":pro_num,
                        "redis_pool": redis_pool,
                        "access_dev": access_dev
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
