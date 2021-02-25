#!/usr/bin/env python3.6
# -*- coding:UTF-8 -*-
# __author__: sdc

import random
import datetime
import json as djson
import confluent_kafka
from sanic import Blueprint
from auth import authorized
from sanic.response import json
from utils.hbaseApi import HbaseManul

import Config
config_env = Config.get_value()

collect = Blueprint("collect")

kafka_ip = config_env["kafka_ip"]
kafka_port = config_env["kafka_port"]
origin_api = config_env["origin_api"]
hbase_ip = config_env["hbase_ip"]
hbase_port = config_env["hbase_port"]
hbase_tube = config_env["hbase_tube"]
logger = config_env["logger"]


def write2Hbase(rowkey, message):
    try:
        hBase = HbaseManul(hbase_ip, hbase_port)
    except Exception as e:
        logger.error(str(e))
        return
    try:
        hBase.switchTable(hbase_tube)
        data = {"info:message": message}
        hBase.putData(rowkey, data)
    except Exception as e:
        logger.error(str(e))
    finally:
        hBase.close()


def ranstr(num):
    H = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*'
    salt = ''
    for i in range(num):
        salt += random.choice(H)
    return salt

@collect.listener('before_server_start')
async def setup_connection(app, loop):
    pass

@collect.listener('after_server_stop')
async def close_connection(app, loop):
    pass

'''
    api数据采集

    接口名称：/api/data/collect

    功能        ：允许通过调用api接口上传日志数据

'''
@collect.route('/api/data/collect', methods=['POST'])
async def asset_update(request):
    try:
        body = djson.loads(request.body.decode())
        data = body["data"] if "data" in body else []
    except Exception as e:
        logger.error(str(e))
        response = {"message": "E2"}
        return json(response)
    for message in data:
        # 上传到kafka服务器
        try:
            object = confluent_kafka.Producer({"bootstrap.servers": "%s:%s" % (kafka_ip, kafka_port)})
            sendData = djson.dumps({"message": str(message)})
            object.produce(origin_api, sendData)
            flag = object.flush(timeout=2)
            if flag != 0:
                logger.error("kafka flush fail")
                response = {"message": "E0"}
                return json(response)
        except Exception as e:
            logger.error(str(e))
            response = {"message": "E0"}
            return  json(response)
        # 上传到hbase服务器
        try:
            rowkey = ranstr(4) + '_' + datetime.datetime.now().strftime("%Y%m%d%H%M%S")[::-1] + '_webapi'
            logger.warning(rowkey)
            write2Hbase(rowkey, sendData)
        except Exception as e:
            logger.error(str(e))
            response = {"message": "E0"}
            return json(response)

    response = {"message": "S0"}
    return json(response)
