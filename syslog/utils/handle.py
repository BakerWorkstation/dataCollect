'''
@Author: your name
@Date: 2020-05-28 11:25:32
LastEditTime: 2020-08-20 10:07:58
LastEditors: Please set LastEditors
@Description: In User Settings Edit
@FilePath: /opt/DataCollect/syslog/utils/handle.py
'''
#!/usr/bin/env python3.6
# -*- coding:utf-8 -*-
# __author__: sdc

import sys
import time
import uuid
import json
import ujson
import random
import datetime
import configparser
from utils.parse import analysis


from struct import unpack
from socket import AF_INET,inet_pton,inet_aton

sys.path.append("/opt/DataCollect")
from append_asset import append_asset



# ------------------------------判断ip是否为内部网络-------------------------------------
def check_private_addr(ip):
        """
        判断ip是否是内网地址，若返回True的话则为内网ip，若返回False则是外部网络ip
        """
        f = unpack('!I', inet_pton(AF_INET, ip))[0]
        '''
        下面网段可选
        '''
        private = (

            [2130706432, 4278190080],  # 127.0.0.0,   255.0.0.0   http://tools.ietf.org/html/rfc3330
            [3232235520, 4294901760],  # 192.168.0.0, 255.255.0.0 http://tools.ietf.org/html/rfc1918
            [2886729728, 4293918720],  # 172.16.0.0,  255.240.0.0 http://tools.ietf.org/html/rfc1918
            [167772160, 4278190080],   # 10.0.0.0,    255.0.0.0   http://tools.ietf.org/html/rfc1918
        )  
        for net in private:
            if (f & net[1]) == net[0]:
                return True
        return False


def ranstr(num):
    H = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*'
    salt = ''
    for i in range(num):
        salt += random.choice(H)
    return salt

def handle_data(producer, message, config_env, cursor):
    logger = config_env["logger"]
    redis_pool = config_env["redis_pool"]
    handle_syslog = config_env["handle_syslog"]
    rule_conf = config_env["rule_conf"]
    tmpdict = {}
    try:
        data = ujson.loads(bytes.decode(message.value().strip()))
    except Exception as e:
        print(str(e))
        tmpdata = bytes.decode(message.value().strip())
        data = {"data": tmpdata, "host": None}
    syslog = data["data"]
    host = data["host"]
    rowkey = ranstr(4) + '_' + datetime.datetime.now().strftime("%Y%m%d%H%M%S")[::-1] + '_syslog'
    tmpdict["rowkey"] = rowkey
    n = syslog.find('>')
    syslog_msg = syslog[(n + 1):]
    #logger.info(syslog_msg)
    flag, result, redisKey = analysis(syslog_msg, logger, host, rule_conf)
    if not flag:
        return (False, redisKey, rowkey, data)
    firename = "model_" + result["fireType"]
    # 从redis中获取防火墙类型对应模板
    try:
        maps = redis_pool.get(firename)
        if not maps:
            fireModelconf = configparser.ConfigParser()
            fireModelconf.read("/opt/DataCollect/syslog/conf/%s.conf" % firename)
            maps = fireModelconf.get("TRANS", "maps")
            redis_pool.set(firename, maps)
    except Exception as e:
        logger.error(str(e))
        return  (False, redisKey, rowkey, data)
    new_maps = json.loads(maps)
    for eachvalue in new_maps.values():
        tmpdict[eachvalue] = ""
    tmpdict["fireHost"] = host
    for key, value in result.items():
        try:
            tmpdict[new_maps[key]] = value
        except:
            tmpdict[key] = value
    serverty = int(syslog[1:n]) & 0x0007
    facility = (int(syslog[1:n]) & 0x03f8) >> 3
    tmpdict["serverty"] = serverty
    tmpdict["facility"] = facility
    #logger.info('-' * 60)
    
    try:
        sendData = ujson.dumps(tmpdict)
        sql = "select asset_id from h_asset_ip_info where ip_addr=%s;"
        for eachip in [tmpdict["sip"], tmpdict["dip"]]:
            flag = check_private_addr(eachip)
            if flag:
                
                cursor.execute(sql, (eachip,))
                old_id = cursor.fetchone()
                if not old_id:
                    asset_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, str(uuid.uuid1()))).replace("-", "").upper()
                    propertykeys = {
                        "asset_id": asset_id,              # 资产标识
                        "node_id": '1',                    # 组织机构标识!!!
                        "type_id": '1',                    # 资产类型(不能"")
                        "asset_level": '3',                # 资产等级
                        "asset_label": '',                 # 资产标签
                        "model": "",                       # 资产型号
                        "host_ip": "",                     # 如果资产分类为虚拟资产则有宿主机ip
                        "position": "",                    # 部署位置
                        "asset_classify": "",              # 资产分类   "1": 虚拟设备  "2": 实体设备
                        "source": "4",                     # 资产来源 "0": iep "1": 人工录入 "2":ptd "3":cmdb "4":防火墙
                        "create_person": host,             # 创建人（此处为防火墙ip）
                        "use_person": "",                  # 使用人
                        "use_contact": "",                 # 使用人联系方式
                        "ip_addr": eachip,                 # ip
                        "mac_addr": "",                    # mac
                        "group_id": "1"                    # 资产组标识  "1":内网资产 "2":外网资产
                    }
                    try:
                        result = append_asset(propertykeys, config_env["pg_conn"])
                        asset_id = result["asset_id"]
                        logger.info('资产注册成功, asset_id->%s' % asset_id)
                        with open("/opt/DataCollect/logs/", "a") as f:
                            f.write(json.dumps(data) + '\n')
                    except Exception as e:
                        pass
                        #logger.error('error:防火墙资产注册失败，message -> %s' % str(e))
                else:
                    pass
                    #logger.info(old_id)
        producer.produce(handle_syslog, sendData)
    except Exception as e:
        logger.error(str(e))
        return (False, redisKey, rowkey, data)
    return (True, redisKey, rowkey, data)