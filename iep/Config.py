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
    server_dir = "/opt/DataCollect/iep/"
    global _global_dict
    try:
        global_conf = configparser.ConfigParser()
        global_conf.read("/opt/DataCollect/global.conf")
        conf = configparser.ConfigParser()
        fileUrl = os.path.join(server_dir, "conf/server.conf")
        conf.read(fileUrl)
        pg_conn = global_conf.get("PG", "pg_conn")
        kafka_ip = global_conf.get("KAFKA", "ip")
        kafka_port = global_conf.get("KAFKA", "port")
        redis_ip = global_conf.get("REDIS", "ip")
        redis_port = int(global_conf.get("REDIS", "port"))
        redis_password = global_conf.get("REDIS", "password")
        redis_db = conf.get("REDIS", "db")
        offset_reset = conf.get("kafka_topic_config", "offset_reset")
        iep_topic = conf.get("kafka_topic", "topic1")
        group_id = conf.get("kafka", "group_id")
        timeout = conf.get("kafka", "timeout")
        max_bytes = conf.get("kafka", "max_bytes")
        hbase_ip = global_conf.get("HBASE", "ip")
        hbase_port = int(global_conf.get("HBASE", "port"))
        log_level = conf.get("LOG", "level")
        pro_num = conf.get("pro_num","process_number")
        server_ip = conf.get("SERVER_IP","server_ip").strip()
        if not server_ip == "-1":
            server_ip = server_ip.split(";")
        # 设置日志输出级别
        if log_level == "info":
            log_level = logging.INFO
        elif log_level == "error":
            log_level = logging.ERROR
        elif log_level == "warning":
            log_level = logging.WARNING
        else:
            log_level = logging.ERROR

        logger = record(filename='iep_server.log', level=log_level)
        # 开启redis_pool 连接池
        pool = redis.ConnectionPool(
        host=redis_ip,
        port=redis_port,
        db=redis_db,
        password=redis_password,
        decode_responses=True
        )
        redis_pool = redis.Redis(connection_pool=pool)
        _global_dict = {
                        "server_dir": server_dir,
                        "pg_conn": pg_conn,
                        "kafka_conn": "%s:%s" % (kafka_ip, kafka_port),
                        "offset_reset": offset_reset,
                        "group_id": group_id,
                        "timeout": timeout,
                        "max_bytes": max_bytes,
                        "hbase_ip": hbase_ip,
                        "hbase_port": hbase_port,
                        "logger": logger,
                        "pro_num":pro_num,
                        "iep_topic":iep_topic,
                        "redis_pool": redis_pool,
                        "server_ip":server_ip
        }
        for i in server_ip:
            ip_is_valid = validate_ip(i)
            if ip_is_valid == False:
                print("server_ip %s is not a valid ip"%(i))
        #print(server_ip)
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

def validate_ip(ip_str):
    sep = ip_str.split('.')
    if len(sep) != 4:
        return False
    for i,x in enumerate(sep):
        try:
            int_x = int(x)
            if int_x < 0 or int_x > 255:
                return False
        except ValueError as e:
            return False
    return True

if __name__ == "__main__":
    _init()
