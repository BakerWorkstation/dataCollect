#!/usr/bin/env python3.6
# -*- coding:utf-8 -*-
# __author__: sdc

import re
import json
import datetime
import configparser

def analysis(message, logger, host, rule_conf):
    redisKey = {}
    manufacturer = ["zhenguan", "hillstone", "star", "gap"]   # 镇关防火墙  山石防火墙设备、启明星辰防火墙设备、网闸设备

    # 按模板库进行基础解析日志，判断日志来源于哪个厂商
    i = 1
    while 1:
        name = "rule%d" % i
        try:
            type_rule = rule_conf.get("TYPE", name)
            res = re.match(type_rule, message.strip())
            info = res.groupdict()
            message1 = info["message"]
            type = info["type"].strip()
            manu_name = manufacturer[i-1]
            break
        except AttributeError:
            i += 1
            continue
        except configparser.NoOptionError:
            return (False, 'configparser Nooption Error, parse error', redisKey)

    device_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if manu_name == "zhenguan":
        redisKey["zhenguan"] = {"origin_count": 1, "black_count": 0, "timestamp": device_timestamp, "device_ip": host}

    if manu_name == "star":
        redisKey["star"] = {"origin_count": 1, "black_count": 0, "timestamp": device_timestamp, "device_ip": host}

    if manu_name == "hillstone":
        redisKey["hillstone"] = {"origin_count": 1, "black_count": 0, "timestamp": device_timestamp, "device_ip": host}

    if manu_name == "gap":
        redisKey["gap"] = {"origin_count": 1, "black_count": 0, "timestamp": device_timestamp, "device_ip": host}

    # 按厂商模板进行详细解析日志，整理好相关的字段属性
    j = 1
    while 1:
        try:
            tmpdict = {}
            name = "rule%d" % j
            #logger.warning(manu_name)
            #logger.warning(name)
            #logger.warning(message.strip())
            rule = rule_conf.get(manu_name, name)
            #logger.warning(message.strip())
            res = re.match(rule, message1.strip())
            data = res.groupdict()
            break
        except AttributeError:
            j += 1
            continue
        except configparser.NoOptionError:
            return (False, 'configparser Nooption Error, parse verbose error', redisKey)

    # 执行到此处，说明日志可以按照模板成功解析
    if manu_name == "star":
        # 启明星辰防火墙要  AV  IPS 类型
        if not type in ["AV", "IPS"]:
            return (False, "type not in AV or IPS", redisKey)
        if 'GenTime' in data:
            tmpdict["GenTime"] = data["GenTime"]
        if 'Content' in data:
            tmpdict["Content"] = data["Content"]
        if 'NatType' in data:
            tmpdict["NatType"] = data["NatType"]
        tmpdata = data["message1"] + ' ' + data["message2"]
        for eachitem in tmpdata.split(" "):
            datalist = (eachitem.split('=', 1))
            tmpdict[datalist[0]] = datalist[-1]
        tmpdict["fireType"] = "1"    # 防火墙类型    "1": 启明
        redisKey["star"]["black_count"] = 1

    if manu_name == "zhenguan":
        tmpdata = data["message1"] + ' ' + data["message2"]
        for eachitem in tmpdata.split(" "):
            datalist = (eachitem.split('=', 1))
            tmpdict[datalist[0]] = datalist[-1]
        tmpdict["fireType"] = "4"    # 防火墙类型    "4": 镇关
        redisKey["zhenguan"]["black_count"] = 1

    # 网闸目前没有详细解析模板
    if manu_name == "gap":
        tmpdict["fireType"] = "3"    # 防火墙类型    "3": 网闸
        redisKey["gap"]["black_count"] = 1

    if manu_name == "hillstone":
        tmpdict = data
        tmpdict["fireType"] = "2"    # 防火墙类型    "2": 山石
        redisKey["hillstone"]["black_count"] = 1

    # 添加每种厂商日志的类型
    tmpdict["logType"] = type

    # 如果源IP为空，则该条日志过滤掉
    if not tmpdict["SrcIP"]:
        return (False, "sip is null", redisKey)

    return (True, tmpdict, redisKey)

if __name__ == '__main__':
    #message = 'Jul 29 07:36:10 48.1.1.206 NAT: SerialNum=100022002000001807193963 GenTime="2019-07-29 15:34:12" NatType="Source NAT" SrcIP=20.1.1.11 SrcPort=137 DstIP=20.1.1.255 DstPort=137 BeforeTransAddr=20.1.1.11 AfterTransAddr=100.1.1.1 Protocol=UDP BeforeTransPort=137 AfterTransPort=137 PolicyID=1 Content="Session Backout" '
    #message = 'Jul 29 07:36:10 48.1.1.206 NAT: SerialNum=100022002000001807193963 GenTime="2019-07-29 15:34:12" SrcIP=20.1.1.11 SrcPort=137 DstIP=20.1.1.255 DstPort=137 BeforeTransAddr=20.1.1.11 AfterTransAddr=100.1.1.1 Protocol=UDP BeforeTransPort=137 AfterTransPort=137 PolicyID=1 Content="Session Backout" '
    #message = ' 2019-07-30 12:50:20, WARNING@MGMT: Admin user "hillstone" logined through SSH, from 172.16.30.4:62434 to 172.16.100.100:22(TCP) '
    #message = ' Jul 29 07:36:10 48.1.1.206 NAT: SerialNum=100022002000001807193963 GenTime="2019-07-29 15:34:12" NatType="Source NAT" SrcIP=20.1.1.11 SrcPort=137 DstIP=20.1.1.255 DstPort=137 BeforeTransAddr=20.1.1.11 AfterTransAddr=100.1.1.1 Protocol=UDP BeforeTransPort=137 AfterTransPort=137 PolicyID=1 Content="Session Backout" '
    message = '<131>charset=[UTF-8] logType=[systemService] serviceName=[TCP\\u901a\\u7528\\u4e2d\\u8f6c\\u670d\\u52a1] desc=[\\u8fde\\u63a5\\u5931\\u8d25[10.1.56.174:22999][Connection refused: /10.1.56.174:22999]] result=[\\u5931\\u8d25] date=[2019-11-05 10:21:55.830]'
    print(analysis(message, ""))
    
