'''
@Author: sdc
@Date: 2019-12-18 11:03:51
@LastEditTime: 2020-06-01 11:09:51
@LastEditors: Please set LastEditors
@Description: IEP relate data to ACD
@FilePath: /opt/DataCollect/iep_asset_sync/consumeAsset.py
'''

#!/usr/bin/env python3.6
# -*- coding:utf-8 -*-
# __author__: sdc

import os
import sys
import time
import json
import uuid
import redis
import base64
import asyncio
import datetime
import psycopg2
import requests
import threading
import psycopg2.extras
import confluent_kafka
from multiprocessing import Process
from DBUtils.PooledDB import PooledDB
from confluent_kafka import Consumer, KafkaError, TopicPartition

from utils.log import record

main_thread_lock = threading.Lock()

sys.path.append("/opt/DataCollect")
from append_asset import append_asset

'''
@description:    获取资产管理中资产的uuid，方便后续对数据中的asset_id进行清洗
@param {type}    cur(object),  olduuid(string)
@return:         True/False,   newuuid(string)
'''
def getAssetId(cur, ip_addr):
    # 判断uuid是否存在资产管理中
    sql = "select ainfo.asset_id from h_hardware_asset_info ainfo left join h_asset_ip_info ipinfo on ainfo.asset_id = ipinfo.asset_id where ipinfo.ip_addr=%s and ainfo.delete_state = '1'; "
    args = (ip_addr, )
    flag = cur.execute(sql, args)
    if flag:
        response = "通用方法: 查询资产管理中资产uuid失败"
        return False, response
    result = cur.fetchone()
    if not result:
        response = "通用方法: IP %s -> 不在资产管理中" % ip_addr
        return False, response
    asset_id = result[0]

    return True, asset_id

'''
@description:    将字符型IP地址转换成整形数值
@param {type}    ip(string)
@return:         value(int)
'''
def convert_ip_to_number(ip_str):
    ret = 0
    ip_str=ip_str.strip()
    parts = ip_str.split('.')
    if len(parts) == 4:
        ret = int(parts[0]) * 256 * 256 * 256 + int(parts[1]) * 256 * 256 + int(parts[2]) * 256  + int(parts[3])
    return ret

'''
@description:    同步IEP组织结构数据
@param {type}    pg_conn(object),  message(string),  config(dict),  logger(object)
@return:         'parse'(消息解析错误)/'error'(消息处理失败)/'success'(消息处理成功),  response(string)
'''
def insertOrg(pg_conn, message, config, logger, elogger):
    logger.info('-' * 60)
    logger.info("开始同步组织结构数据")
    cur = pg_conn.cursor()
    response = ""
    try:
        tmpdict = json.loads(message)
        server_ip = tmpdict['server_ip']
        if not config["limit_device"] == "-1" and not server_ip in config["limit_device"]:
            logger.warning("server_ip -> %s" % server_ip)
            pg_conn.commit()
            return 'success', response
        server_org = tmpdict["org"]
        server_uid = tmpdict["server_uid"]
        logger.info(message)
    except Exception as e:
        elogger.error(str(e))
        response = "同步组织结构: 数据格式解析错误"
        pg_conn.commit()
        return 'parse', response

    # 处理子公司数据
    for eachid in server_org:
        info = tmpdict[str(eachid)]
        p_node_cid = str(info["pid"])
        if not p_node_cid == "0":
            p_node_cid = str(info["pid"]) + "_%s" % server_ip
        else:
            p_node_cid =  "1"
        node_cid = str(info["id"]) + "_%s" % server_ip
        cname = info["cname"]
        group = info["group"]
        sql = '''insert into sys_node_info(node_id, node_type_id, node_name, parent_node_id, state) 
                    values (%s, %s, %s, %s, %s) 
                  on conflict(node_id) do update set node_name=%s, parent_node_id=%s; '''

        # 处理子公司下部门数据
        #logger.info(group)
        for eachgroup in group:
            p_node_gid = str(eachgroup["pid"]) + "_%s" % server_ip
            node_gid = str(eachgroup["gid"]) + "_%s" % server_ip
            gname = eachgroup["gname"]
            #if str(eachgroup["pid"]) == str(info["pid"]) and gname == cname:
            #    continue
            sql = '''insert into sys_node_info(node_id, node_type_id, node_name, parent_node_id, state) 
                        values (%s, %s, %s, %s, %s) 
                      on conflict(node_id) do update set node_name=%s, parent_node_id=%s; '''
            args = (node_gid, 2, gname, p_node_gid, "1", gname, p_node_gid, )
            try:
                cur.execute(sql, args)
            except Exception as e:
                elogger.error(str(e))
                pg_conn.commit()
                response = "同步组织结构: 插入部门级组织结构数据失败"
                return 'error', response
            logger.info("同步部门组织完成 -> 名称: %s\tID: %s" % (gname, node_gid))

        # 处理公司数据
        args = (node_cid, 1, cname, p_node_cid, "1", cname, p_node_cid, )
        try:
            cur.execute(sql, args)
        except Exception as e:
            elogger.error(str(e))
            pg_conn.commit()
            response = "同步组织结构: 插入公司级组织结构数据失败"
            return 'error', response
        logger.info("同步公司组织完成 -> 名称: %s\tID: %s" % (cname, node_cid))
    pg_conn.commit()

    logger.info("组织结构数据同步完成")
    return 'success', response


'''
@description:    同步IEP资产数据
@param {type}    pg_conn(object),  message(string),  config(dict),  logger(object)
@return:         'parse'(消息解析错误)/'error'(消息处理失败)/'success'(消息处理成功),  response(string)
'''
def insertAsset(pg_conn, message, config, logger, elogger):
    logger.info('-' * 60)
    logger.info("开始同步资产数据")
    cur = pg_conn.cursor()
    response = ""
    try:
        tmpdict = json.loads(message)
        server_ip = tmpdict['server_ip']
        if not config["limit_device"] == "-1" and not server_ip in config["limit_device"]:
            logger.warning("server_ip -> %s" % server_ip)
            pg_conn.commit()
            return 'success', response
        server_uid = tmpdict['server_uid']
        update_person = server_uid
        create_person = server_uid
        update_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        iep_asset_id = tmpdict["uuid"]
        node_id = str(tmpdict["group_id"]) + "_%s" % server_ip
        sysver = tmpdict["sysver"]
        address = tmpdict["address"]
        # 根据资产status状态更新iep_uuid
        status = tmpdict["status"]
        if status == 'install':
            delete_state = '1'  # 在用
        else:
            # 资产状态为删除时，需要将IEP UUID置为空
            delete_state = '0'  # 删除
            server_uid = None
            logger.warning("状态为删除 -> IEP_UUID置为空。")
        logger.info(message)
    except Exception as e:
        pg_conn.commit()
        elogger.error(str(e))
        response = "同步资产: 数据格式解析错误"
        return 'parse', response

    # 通过IP判断资产是否已经存在，如存在 -> 将IEP资产合并其中，不存在 -> 注册资产
    asset_id = None
    sql = "select ainfo.asset_id from h_hardware_asset_info ainfo left join h_asset_ip_info ipinfo on ainfo.asset_id = ipinfo.asset_id where ipinfo.ip_addr=%s"
    for eachaddress in address:
        ip = eachaddress["ip"]
        mac = eachaddress["mac"]
        args = (ip.strip(), )
        try:
            cur.execute(sql, args)
        except Exception as e:
            elogger.error(str(e))
            pg_conn.commit()
            response = "同步资产: 通过ip地址查询资产管理资产uuid失败"
            return False, response
        ipresult = cur.fetchone()
        if ipresult:
            asset_id = ipresult[0]
            break
    if not asset_id:
        asset_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, str(uuid.uuid1()))).replace("-", "").upper()
    else:
        # 存在ACD资产， 查看映射的IEP资产是否存在
        sql = "select map_asset_id from h_asset_source_record where first_asset_id=%s and asset_source = '0'"
        args = (asset_id, )
        try:
            cur.execute(sql, args)
        except Exception as e:
            elogger.error(str(e))
            pg_conn.commit()
            response = "同步资产: 通过ACD-uuid查询IEP-uuid失败"
            return False, response
        uuidresult = cur.fetchone()
        if uuidresult:
            map_asset_id = uuidresult[0]
            # 相同IP iep上报的两次uuid不一样
            if not map_asset_id == iep_asset_id:
                sql = "update h_asset_source_record set map_asset_id=%s where first_asset_id=%s and asset_source='0';"
                args = (iep_asset_id, asset_id, )
                try:
                    cur.execute(sql, args)
                except Exception as e:
                    elogger.error(str(e))
                    pg_conn.commit()
                    response = "同步资产: 将多源表中IEP-uuid更新失败"
                    return False, response

    logger.info("资产管理 -> %s   IEP -> %s" % (asset_id, iep_asset_id))


    # iep资产的uuid信息记录到资产多源历史同步表中做为外键, 资产管理生成新的资产uuid作为主键  (postgresql 多线程数据同步)
    sql = '''with w1 as(select ('x'||substr(md5(%s),1,16))::bit(64)::bigint as tra_id),    
                upsert as (update h_asset_source_record set r_device=%s, r_time=%s where map_asset_id=%s and asset_source=%s returning *)    
                    insert into h_asset_source_record(first_asset_id, asset_source, map_asset_id, r_device, r_time ) select %s, %s, %s, %s, %s from w1   
                 where pg_try_advisory_xact_lock(tra_id) and not exists (select 1 from upsert where map_asset_id=%s); '''
    args = (iep_asset_id, server_uid, update_time, iep_asset_id, '0', asset_id, '0', iep_asset_id, server_uid, update_time, iep_asset_id, )
    try:
        cur.execute(sql, args)
    except Exception as e:
        elogger.error(str(e))
        pg_conn.commit()
        response = "同步资产: 同步资产多源历史失败"
        return 'error', response

    # 资产系统版本变化，需要同步到漏洞系统
    sql = "select os_ver from h_hardware_asset_info where asset_id=%s; "
    args = (asset_id, )
    try:
        cur.execute(sql, args)
    except Exception as e:
        elogger.error(str(e))
        pg_conn.commit()
        response = "同步资产: 查询资产系统版本失败"
        return 'error', response
    oldver = cur.fetchone()
    if oldver:
        osver = oldver[0]
        if not osver == sysver:
            logger.warning('系统版本变化，需要同步到漏洞')
            try:
                object = confluent_kafka.Producer({"bootstrap.servers": "%s:%s" % (config["kafka_ip"], config["kafka_port"])})
                sendData = {"uuid": asset_id, "type": 'systemVer', "old": osver, "new": sysver}
                object.produce("asset_status", json.dumps(sendData))
                object.flush(timeout=1)
            except Exception as e:
                elogger.error(str(e))

    # 组织结构未上报时, 需要先同步上资产里的iep_uuid(外键), 后期可屏蔽此代码
    if server_uid:
        sql = "insert into k_iep_server_info values (%s, %s) on conflict(iep_uuid) do update set iep_server_ip=%s; "
        args = (server_uid, server_ip, server_ip, )
        try:
            cur.execute(sql, args)
        except Exception as e:
            elogger.error(str(e))
            pg_conn.commit()
            response = "同步资产: 插入组织结构数据失败"
            return 'error', response

    # 判断资产信息中的组织结构数据，在数据库中是否存在， 如不存在， 重置node_id="1"
    sql = "select node_id from sys_node_info where node_id=%s and state='1'; "
    args = (node_id, )
    try:
        cur.execute(sql, args)
    except Exception as e:
        elogger.error(str(e))
        pg_conn.commit()
        response = "同步资产: 查询资产数据失败"
        return 'error', response
    noderesult = cur.fetchone()
    if not noderesult:
        logger.warning("资产组织结构不存在 -> 置为'1'。组织结构: %s" % node_id)
        node_id = "1"

    # 插入、更新资产数据， IEP同步的资产为未登记资产，创建人、更新人为iep服务端的uuid
    sql = '''insert into h_hardware_asset_info(asset_id, node_id, iep_uuid, os_ver, asset_level, register_state, delete_state, fall_state, source, create_person) 
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
             on conflict(asset_id) do update set node_id=%s, iep_uuid=%s, os_ver=%s, delete_state=%s, asset_perfect_source=%s, update_person=%s, update_time=%s; '''
    args = (asset_id, node_id, server_uid, sysver, 3, '0', delete_state, '0', '0', create_person, node_id, server_uid, sysver, delete_state, '0', update_person, update_time, )
    try:
        cur.execute(sql, args)
    except Exception as e:
        elogger.error(str(e))
        pg_conn.commit()
        response = "同步资产: 插入资产数据失败"
        return 'error', response
    logger.info("同步资产 -> 更新资产成功。")

    # 同步资产的IP、MAC信息  (postgresql 多线程数据同步)
    sql = '''with w1 as(select ('x'||substr(md5(%s),1,16))::bit(64)::bigint as tra_id),    
                upsert as (update h_asset_ip_info ipinfo set r_person=%s, r_time=%s from h_hardware_asset_info ainfo where ipinfo.asset_id=ainfo.asset_id and ipinfo.ip_addr=%s and ainfo.asset_id=%s and ainfo.delete_state='1' returning *)    
                    insert into h_asset_ip_info(asset_id, group_id, ip_addr, ip_value, mac_addr, r_person, r_time) select %s, %s, %s, %s, %s, %s, %s from w1   
                 where pg_try_advisory_xact_lock(tra_id) and not exists (select 1 from upsert where ip_addr=%s); '''
    for eachaddress in address:
        ip = eachaddress["ip"]
        mac = eachaddress["mac"]
        args = (asset_id, server_uid, update_time, ip, asset_id, asset_id, '1', ip, convert_ip_to_number(ip), mac, server_uid, update_time, ip, )
        try:
            cur.execute(sql, args)
        except Exception as e:
            elogger.error(str(e))
            pg_conn.commit()
            response = "同步资产: 插入资产IP数据失败"
            return 'error', response
    logger.info("同步资产 -> 更新资产IP地址成功。 IP: %s" % str(address))
    #sql = "select 1 from h_hardware_asset_info ainfo left join h_asset_ip_info ipinfo on ainfo.asset_id = ipinfo.asset_id where ipinfo.ip_addr is not null and ainfo.delete_state = '1' and ipinfo.asset_id=%s"
    #args = (asset_id, )
    #try:
    #    cur.execute(sql, args)
    #except Exception as e:
    #    elogger.error(str(e))
    #    pg_conn.commit()
    #    response = "同步资产: 查询资产录入的IP数据失败"
    #    return 'error', response
    #result = cur.fetchone()
    #if not result:
    #    sql = "delete from h_hardware_asset_info where asset_id=%s"
    #    args = (asset_id, )
    #    try:
    #        cur.execute(sql, args)
    #    except Exception as e:
    #        elogger.error(str(e))
    #        pg_conn.commit()
    #        response = "同步资产: 删除空IP资产失败"
    #        return 'error', response
    pg_conn.commit()

    logger.info("资产数据同步完成")
    return 'success', response


'''
@description:    同步IEP软件数据
@param {type}    pg_conn(object),  message(string),  config(dict),  logger(object)
@return:         'parse'(消息解析错误)/'error'(消息处理失败)/'success'(消息处理成功),  response(string)
'''
def insertSoft(pg_conn, message, config, logger, elogger):
    logger.info('-' * 60)
    logger.info("开始同步软件数据")
    cur = pg_conn.cursor()
    response = ""
    try:
        tmpdict = json.loads(message)
        asset_id = tmpdict["client"]["uuid"]
        server_uid = tmpdict["client"]["server_uid"]
        ip_addr = tmpdict["client"]["ip"]
        server_ip = tmpdict["client"]["server_ip"]
        if not config["limit_device"] == "-1" and not server_ip in config["limit_device"]:
            logger.warning("server_ip -> %s" % server_ip)
            pg_conn.commit()
            return 'success', response
        software = tmpdict["softwareinfo"]
        logger.info(message)
    except Exception as e:
        pg_conn.commit()
        elogger.error(str(e))
        response = "同步软件: 数据格式解析错误"
        return 'parse', response

    # 获取资产管理中资产的uuid
    flag, asset_id = getAssetId(cur, ip_addr)
    if not flag:
        pg_conn.commit()
        return 'error', asset_id
    logger.info("资产 -> %s" % asset_id)

    # 同步资产的软件信息  (postgresql 多线程数据同步)
    # 获取资产全量软件列表
    sql = 'select array_agg(sinfo.software_name) from h_asset_software asoft left join h_software_info sinfo on asoft.software_id=sinfo.software_id where asset_id = %s'
    args = (asset_id, )
    try:
        cur.execute(sql, args)
    except Exception as e:
        elogger.error(str(e))
        pg_conn.commit()
        response = "同步软件: 获取资产所有软件失败"
        return 'error', response
    result = cur.fetchone()
    if result:
        softs = result[0]
    else:
        softs = []
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # 处理软件数据
    for eachsoftware in software:
        software_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, str(uuid.uuid1()))).replace("-", "").upper()
        soft_status = '1'  # 状态写死1   已安装
        soft_path = eachsoftware.get('path')
        soft_ver = eachsoftware.get('ver')
        soft_name = eachsoftware.get('name')
        if '??' in soft_path or '??' in soft_ver or '??' in soft_name:
            continue
        soft_instime = eachsoftware.get('instime')
        pulish = eachsoftware.get('author')
        # 插入、更新软件库表
        sql = '''with w1 as(select ('x'||substr(md5(%s),1,16))::bit(64)::bigint as tra_id),    
                    upsert as (update h_software_info set state= case state when '0' then '1' else state end, software_source=%s, update_person=%s, update_time=%s where software_name=%s returning *)    
                        insert into h_software_info(software_id,  parent_software_id, software_type_id, software_name, software_source, publisher, state, register_person, register_time) 
                            select %s, %s, %s, %s, %s, %s, %s, %s, %s from w1
                          where pg_try_advisory_xact_lock(tra_id) and not exists (select 1 from upsert where software_name=%s); '''
        args = (soft_name, '2', server_uid, timestamp, soft_name, software_id, software_id, 3, soft_name, '2', pulish, '1', server_uid, timestamp, soft_name, )
        try:
            cur.execute(sql, args)
        except Exception as e:
            elogger.error(str(e))
            pg_conn.commit()
            response = "同步软件: 插入、更新软件库表失败。软件: %s" % soft_name
            return 'error', response
        logger.info("插入、更新: 软件新增成功。软件: %s" % soft_name)
        # 插入、更新软件库流水表
        sql = '''
                 insert into h_asset_software_stream (asset_id, software_type_id, software_name, install_addr, version, install_time, r_time) values(%s, %s, %s, %s, %s, %s ,%s);
        '''
        args = (asset_id, '3', soft_name, soft_path, soft_ver, soft_instime, timestamp, )
        try:
            cur.execute(sql, args)
        except Exception as e:
            elogger.error(str(e))
            pg_conn.commit()
            response = "同步软件: 插入、更新资产-软件流水表失败。 软件: %s" % soft_name
            return 'error', response
        logger.info("插入、更新: 资产-软件流水表对应关系新增成功。")
        #  插入资产-软件表(非流水)
        sql = '''with w1 as(select ('x'||substr(md5(%s),1,16))::bit(64)::bigint as tra_id),    
                    upsert as (update h_asset_software asoft set source=%s, install_addr=%s, version=%s, install_time=%s, install_state=%s, update_person=%s, update_time=%s from h_software_info as sinfo where sinfo.software_id=asoft.software_id and asset_id=%s and software_name=%s returning *)    
		                insert into h_asset_software(asset_id,  software_id, source, install_addr, version, install_time, install_state, register_person, register_time) select %s, (select software_id from h_software_info where software_name=%s), %s, %s, %s, %s, %s, %s, %s from w1   
	              where pg_try_advisory_xact_lock(tra_id) and not exists (select 1 from upsert where software_name=%s); '''
        args = (soft_name, '2', soft_path, soft_ver , soft_instime, '1', server_uid, timestamp, asset_id, soft_name, asset_id, soft_name, '2', soft_path, soft_ver , soft_instime, '1', server_uid, timestamp, soft_name, )
        try:
            cur.execute(sql, args)
        except Exception as e:
            elogger.error(str(e))
            pg_conn.commit()
            response = "同步软件: 插入、更新资产-软件表失败。 软件: %s" % soft_name
            return 'error', response
        logger.info("插入、更新: 资产-软件表对应关系新增成功。")

        try:
            softs.remove(soft_name)
        except:
            pass
    # 将本次未上报的软件状态置成 -> 卸载
    if softs:
        sql = '''update h_asset_software asoft set install_state='0' from h_software_info as sinfo 
                    where asoft.software_id=sinfo.software_id and asoft.asset_id=%s 
                         and sinfo.software_name in (select unnest(array[%s]))'''
        args = (asset_id, softs, )
        try:
            cur.execute(sql, args)
        except Exception as e:
            elogger.error(str(e))
            pg_conn.commit()
            response = "同步软件: 将未上报软件状态更新为卸载失败。软件: %s" % str(softs)
            return 'error', response
        logger.info("同步软件: 已卸载软件列表 -> %s" % str(softs))
    pg_conn.commit()

    logger.info("软件数据同步完成")
    return 'success', response


'''
@description:    同步IEP硬件数据 先删除，再插入   (postgresql 多线程数据同步)
@param {type}    pg_conn(object),  message(string),  config(dict),  logger(object)
@return:         'parse'(消息解析错误)/'error'(消息处理失败)/'success'(消息处理成功),  response(string)
'''
def insertHard(pg_conn, message, config, logger, elogger):
    logger.info('-' * 60)
    logger.info("开始同步硬件数据")
    cur = pg_conn.cursor()
    response = ""
    try:
        tmpdict = json.loads(message)
        asset_id = tmpdict["client"]["uuid"]
        ip_addr = tmpdict["client"]["ip"]
        server_ip = tmpdict["client"]["server_ip"]
        if not config["limit_device"] == "-1" and not server_ip in config["limit_device"]:
            logger.warning("server_ip -> %s" % server_ip)
            pg_conn.commit()
            return 'success', response
        cpuinfo = tmpdict["data"][0]     # cpu
        bandinfo = tmpdict["data"][1]    # 主板
        sysinfo = tmpdict["data"][2]     # 系统  暂不用
        showinfo = tmpdict["data"][3]    # 显卡
        meminfo = tmpdict["data"][4]     # 内存
        netinfo = tmpdict["data"][5]     # 网卡
        diskinfo = tmpdict["data"][6]    # 硬盘
        driveinfo = tmpdict["data"][7]   # 光驱
        soundinfo = tmpdict["data"][8]   # 声卡
        logger.info(message)
    except Exception as e:
        pg_conn.commit()
        elogger.error(str(e))
        response = "同步硬件: 数据格式解析错误"
        return 'parse', response

    # 获取资产管理中资产的uuid
    flag, asset_id = getAssetId(cur, ip_addr)
    if not flag:
        pg_conn.commit()
        return 'error', asset_id
    logger.info("资产 -> %s" % asset_id)

    # 同步资产的CPU信息
    if cpuinfo:
        cpuname = cpuinfo["Name"]
        thread = cpuinfo["linenum"]
        core = cpuinfo["NumberOfCores"]
        iftype = cpuinfo["SocketDesignation"]
        sql = '''with w1 as(select ('x'||substr(md5(%s),1,16))::bit(64)::bigint as tra_id),    
                    upsert as (update h_hardware_detail set thread_num=%s, core_num=%s, interface_type=%s where asset_id=%s and hardware_type_id=%s and brand=%s returning *)    
                        insert into h_hardware_detail(asset_id, hardware_type_id, brand, thread_num, core_num, interface_type) select %s, %s, %s, %s, %s ,%s from w1   
                      where pg_try_advisory_xact_lock(tra_id) and not exists (select 1 from upsert where brand=%s); '''
        args = (cpuname, thread, core, iftype, asset_id, 1, cpuname, asset_id, 1, cpuname, thread, core, iftype, cpuname, )
        try:
            cur.execute(sql, args)
        except Exception as e:
            elogger.error(str(e))
            pg_conn.commit()
            response = "同步硬件: 更新资产CPU数据失败"
            return 'error', response
        pg_conn.commit()
        logger.info("同步硬件 -> 更新CPU成功。")

    # 同步资产的主板信息
    if bandinfo:
        bandmanufacturer = bandinfo["Manufacturer"]
        bandserial = bandinfo["SerialNumber"]
        if bandserial == "None":
            bandserial = None
        band_version = bandinfo["Version"]
        if band_version == "None":
            band_version = None
        bios_ver = bandinfo["BIOSVersion"]
        bios_time = bandinfo["BIOSReleaseDate"]
        if bios_time == "None":
            bios_time = None
        if not bios_time:
            bios_time = None
        sql = '''with w1 as(select ('x'||substr(md5(%s),1,16))::bit(64)::bigint as tra_id),    
                    upsert as (update h_hardware_detail set serial_number=%s, version=%s, bios_version=%s, bios_time=%s where asset_id=%s and hardware_type_id=%s and bios_version=%s returning *)    
                        insert into h_hardware_detail(asset_id, hardware_type_id, manufacturer, serial_number, version, bios_version, bios_time) select %s, %s, %s, %s, %s ,%s, %s from w1   
                      where pg_try_advisory_xact_lock(tra_id) and not exists (select 1 from upsert where bios_version=%s); '''
        args = (bios_ver, bandserial, band_version, bios_ver, bios_time, asset_id, 2, bios_ver, asset_id, 2, bandmanufacturer, bandserial, band_version, bios_ver, bios_time, bios_ver, )
        try:
            cur.execute(sql, args)
        except Exception as e:
            elogger.error(str(e))
            pg_conn.commit()
            response = "同步硬件: 更新资产主板数据失败"
            return 'error', response
        pg_conn.commit()
        logger.info("同步硬件 -> 更新主板成功。")

    # 同步资产的系统信息，暂跳过 不同步
    if sysinfo:
        pass

    # 同步资产的显卡信息  --列表
    if showinfo:
        for eachshow in showinfo:
            caption = eachshow["Caption"]
            adapRam = eachshow["AdapterRAM"]
            dversion = eachshow["DriverVersion"]
            sql = '''with w1 as(select ('x'||substr(md5(%s),1,16))::bit(64)::bigint as tra_id),    
                        upsert as (update h_hardware_detail set gcard_memory=%s, driver_version=%s where asset_id=%s and hardware_type_id=%s and brand=%s returning *)    
                            insert into h_hardware_detail(asset_id, hardware_type_id, brand, gcard_memory, driver_version) select %s, %s, %s, %s, %s from w1   
                         where pg_try_advisory_xact_lock(tra_id) and not exists (select 1 from upsert where brand=%s); '''
            args = (caption, adapRam, dversion, asset_id, 6, caption, asset_id, 6, caption, adapRam, dversion, caption, )
            try:
                cur.execute(sql, args)
            except Exception as e:
                elogger.error(str(e))
                pg_conn.commit()
                response = "同步硬件: 更新资产显卡数据失败"
                return 'error', response
        pg_conn.commit()
        logger.info("同步硬件 -> 更新显卡成功。")

    # 同步资产的内存信息  --列表
    if meminfo:
        for eachmem in meminfo:
            memmanufacturer = eachmem["Manufacturer"]
            memname = eachmem["PartNumber"]
            memserial = eachmem["SerialNumber"]
            speed = eachmem["Speed"]
            memcapacity = eachmem["Capacity"]
            cur.execute(sql, args)
            sql = '''with w1 as(select ('x'||substr(md5(%s),1,16))::bit(64)::bigint as tra_id),    
                        upsert as (update h_hardware_detail set brand=%s, serial_number=%s, frequency=%s, capacity=%s where asset_id=%s and hardware_type_id=%s and manufacturer=%s returning *)    
                            insert into h_hardware_detail(asset_id, hardware_type_id, manufacturer, brand, serial_number, frequency, capacity) select %s, %s, %s, %s, %s, %s, %s from w1   
                         where pg_try_advisory_xact_lock(tra_id) and not exists (select 1 from upsert where manufacturer=%s); '''
            args = (memmanufacturer, memname, memserial, speed, memcapacity, asset_id, 3 , memmanufacturer, asset_id, 3, memmanufacturer, memname, memserial, speed, memcapacity, memmanufacturer, )
            try:
                cur.execute(sql, args)
            except Exception as e:
                elogger.error(str(e))
                pg_conn.commit()
                response = "同步硬件: 更新资产内存数据失败"
                return 'error', response
        pg_conn.commit()
        logger.info("同步硬件 -> 更新内存成功。")

    # 同步资产的网卡信息  --列表
    if netinfo:
        for eachnet in netinfo:
            netmanufacturer = eachnet["Manufacturer"]
            netname = eachnet["Name"]
            mac =  eachnet["MACAddress"]
            nettype = eachnet["NetConnectionID"]
            netenable = eachnet["NetEnabled"]
            if netenable == "True":
                netenable = "1"
            else:
                netenable = "0"
            sql = '''with w1 as(select ('x'||substr(md5(%s),1,16))::bit(64)::bigint as tra_id),    
                        upsert as (update h_hardware_detail set brand=%s, mac_addr=%s, net_card_type=%s, is_connecte=%s where asset_id=%s and hardware_type_id=%s and manufacturer=%s returning *)    
                            insert into h_hardware_detail(asset_id, hardware_type_id, manufacturer, brand, mac_addr, net_card_type, is_connecte) select %s, %s, %s, %s, %s, %s, %s from w1   
                         where pg_try_advisory_xact_lock(tra_id) and not exists (select 1 from upsert where manufacturer=%s); '''
            args = (netmanufacturer, netname, mac, nettype, netenable, asset_id, 5 , netmanufacturer, asset_id, 5, netmanufacturer, netname, mac, nettype, netenable, netmanufacturer, )
            try:
                cur.execute(sql, args)
            except Exception as e:
                elogger.error(str(e))
                pg_conn.commit()
                response = "同步硬件: 更新资产网卡数据失败"
                return 'error', response
        pg_conn.commit()
        logger.info("同步硬件 -> 更新网卡成功。")

    # 同步资产的硬盘信息 --列表
    if diskinfo:
        for eachdisk in diskinfo:
            diskname = eachdisk["Caption"]
            firmware = eachdisk["FirmwareRevision"]
            size = eachdisk["Size"]
            try:
                size = '%sG' % int(int(size)/1024/1024/1024)
            except:
                pass
            sql = '''with w1 as(select ('x'||substr(md5(%s),1,16))::bit(64)::bigint as tra_id),    
                        upsert as (update h_hardware_detail set firmware_version=%s, capacity=%s where asset_id=%s and hardware_type_id=%s and brand=%s returning *)    
                            insert into h_hardware_detail(asset_id, hardware_type_id, brand, firmware_version, capacity) select %s, %s, %s, %s, %s from w1   
                         where pg_try_advisory_xact_lock(tra_id) and not exists (select 1 from upsert where brand=%s); '''
            args = (diskname, firmware, size, asset_id, 4, diskname, asset_id, 4, diskname, firmware, size, diskname, )
            try:
                cur.execute(sql, args)
            except Exception as e:
                elogger.error(str(e))
                pg_conn.commit()
                response = "同步硬件: 更新资产硬盘数据失败"
                return 'error', response
        pg_conn.commit()
        logger.info("同步硬件 -> 更新硬盘成功。")

    # 同步资产的光驱信息 --列表
    if driveinfo:
        for eachdrive in driveinfo:
            drivename = eachdrive["Caption"]
            drivetype = eachdrive["MediaType"]
            driveS = eachdrive["Drive"]
            sql = '''with w1 as(select ('x'||substr(md5(%s),1,16))::bit(64)::bigint as tra_id),    
                        upsert as (update h_hardware_detail set cd_type=%s where asset_id=%s and hardware_type_id=%s and brand=%s and drive_letter=%s returning *)    
                            insert into h_hardware_detail(asset_id, hardware_type_id, brand, cd_type, drive_letter) select %s, %s, %s, %s, %s from w1   
                         where pg_try_advisory_xact_lock(tra_id) and not exists (select 1 from upsert where brand=%s); '''
            args = (drivename, drivetype, asset_id, 8, drivename, driveS, asset_id, 8, drivename, drivetype, driveS, drivename, )
            try:
                cur.execute(sql, args)
            except Exception as e:
                elogger.error(str(e))
                pg_conn.commit()
                response = "同步硬件: 更新资产光驱数据失败"
                return 'error', response
        pg_conn.commit()
        logger.info("同步硬件 -> 更新光驱成功。")

    # 同步资产的声卡信息 --列表
    if soundinfo:
        for eachsound in soundinfo:
            soundmanufacturer = eachsound["Manufacturer"]
            soundname = eachsound["Caption"]
            sql = '''with w1 as(select ('x'||substr(md5(%s),1,16))::bit(64)::bigint as tra_id),    
                        upsert as (update h_hardware_detail set manufacturer=%s where asset_id=%s and hardware_type_id=%s and brand=%s returning *)    
                            insert into h_hardware_detail(asset_id, hardware_type_id, manufacturer, brand) select %s, %s, %s, %s from w1   
                         where pg_try_advisory_xact_lock(tra_id) and not exists (select 1 from upsert where brand=%s); '''
            args = (soundname, soundmanufacturer, asset_id, 7, soundname, asset_id, 7, soundmanufacturer, soundname, soundname, )
            try:
                cur.execute(sql, args)
            except Exception as e:
                elogger.error(str(e))
                pg_conn.commit()
                response = "同步硬件: 更新资产声卡数据失败"
                return 'error', response
        pg_conn.commit()
        logger.info("同步硬件 -> 更新声卡成功。")

    logger.info("硬件数据同步完成")
    return 'success', response


'''
@description:    同步IEP服务数据 先删除，再插入   (postgresql 多线程数据同步)
@param {type}    pg_conn(object),  message(string),   config(dict),  logger(object)
@return:         'parse'(消息解析错误)/'error'(消息处理失败)/'success'(消息处理成功),  response(string)
'''
def insertServ(pg_conn, message, config, logger, elogger):
    logger.info('-' * 60)
    logger.info("开始同步服务数据")
    cur = pg_conn.cursor()
    response = ""
    try:
        tmpdict = json.loads(message)
        asset_id = tmpdict["client"]["uuid"]
        servicedata = tmpdict["serviceinfo"]
        ip_addr = tmpdict["client"]["ip"]
        server_ip = tmpdict["client"]["server_ip"]
        if not config["limit_device"] == "-1" and not server_ip in config["limit_device"]:
            logger.warning("server_ip -> %s" % server_ip)
            pg_conn.commit()
            return 'success', response
    except Exception as e:
        pg_conn.commit()
        elogger.error(str(e))
        response = "同步服务: 数据格式解析错误"
        return 'parse', response
    
    # 获取资产管理中资产的uuid
    flag, asset_id = getAssetId(cur, ip_addr)
    if not flag:
        pg_conn.commit()
        return 'error', asset_id
    logger.info("资产 -> %s" % asset_id)

    # 同步资产的服务信息 --列表
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for eachservice in servicedata:
        service_state = eachservice["status"]
        service_name = eachservice["service"]
        show_name = eachservice["name"]
        service_path = eachservice["path"].replace('"', '').split(" ")[0].replace('', "").strip()
        service_method = eachservice["class"]
        r_time = eachservice["systime"]
        sql = "insert into h_asset_service(asset_id, service_name, service_path, service_state, service_method, show_name, r_time) values (%s, %s, %s, %s, %s, %s, %s);"
        args = (asset_id, service_name, service_path, service_state, service_method, show_name, timestamp, )
        try:
            cur.execute(sql, args)
        except Exception as e:
            elogger.error(str(e))
            pg_conn.commit()
            response = "同步服务: 插入资产服务数据失败"
            return 'error', response
    pg_conn.commit()

    logger.info("服务数据同步完成")
    return 'success', response


'''
@description:    同步IEP进程数据 先删除，再插入   (postgresql 多线程数据同步)
@param {type}    pg_conn(object),  message(string),   config(dict),  logger(object)
@return:         'parse'(消息解析错误)/'error'(消息处理失败)/'success'(消息处理成功),  response(string)
'''
def insertProc(pg_conn, message, config, logger, elogger):
    logger.info('-' * 60)
    logger.info("开始同步进程数据")
    cur = pg_conn.cursor()
    response = ""
    try:
        tmpdict = json.loads(message)
        asset_id = tmpdict["client"]["uuid"]
        procdata = tmpdict["porcinfos"]
        ip_addr = tmpdict["client"]["ip"]
        server_ip = tmpdict["client"]["server_ip"]
        if not config["limit_device"] == "-1" and not server_ip in config["limit_device"]:
            logger.warning("server_ip -> %s" % server_ip)
            pg_conn.commit()
            return 'success', response
    except Exception as e:
        pg_conn.commit()
        elogger.error(str(e))
        response = "同步进程: 数据格式解析错误"
        return 'parse', response
    
    # 获取资产管理中资产的uuid
    flag, asset_id = getAssetId(cur, ip_addr)
    if not flag:
        pg_conn.commit()
        return 'error', asset_id
    logger.info("资产 -> %s" % asset_id)

    # 同步资产的进程信息 --列表
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for eachservice in procdata:
        proc_state = eachservice["status"]
        proc_pid = eachservice["pid"]
        proc_path = eachservice["proc"]
        r_time = eachservice["systime"]
        sql = "insert into h_asset_process(asset_id, pid, process_path, state, r_time) values (%s, %s, %s, %s, %s);"
        args = (asset_id, proc_pid, proc_path, proc_state, timestamp, )
        try:
            cur.execute(sql, args)
        except Exception as e:
            elogger.error(str(e))
            pg_conn.commit()
            response = "同步进程: 插入资产服务数据失败"
            return 'error', response
    pg_conn.commit()

    logger.info("进程数据同步完成")
    return 'success', response


'''
@description:    漏洞数据清洗，资产UUID更新   (postgresql 多线程数据同步)
@param {type}    pg_conn(object),  message(string),   config(dict),  logger(object)
@return:         'parse'(消息解析错误)/'error'(消息处理失败)/'success'(消息处理成功),  response(string)
'''
def insertHole(pg_conn, message, config, logger, elogger):
    logger.info('-' * 60)
    logger.info("开始同步漏洞数据")
    cur = pg_conn.cursor()
    response = ""
    try:
        tmpdict = json.loads(message)
        asset_id = tmpdict["client"]["uuid"]
        server_uid = tmpdict["client"]["server_uid"]
        ip = tmpdict["client"]["ip"]
        server_ip = tmpdict["client"]["server_ip"]
        if not config["limit_device"] == "-1" and not server_ip in config["limit_device"]:
            logger.warning("server_ip -> %s" % server_ip)
            pg_conn.commit()
            return 'success', response
    except Exception as e:
        pg_conn.commit()
        elogger.error(str(e))
        response = "同步漏洞: 数据格式解析错误"
        return 'parse', response

    # 获取资产管理中资产的uuid
    flag, message = getAssetId(cur, ip)
    if not flag:
        # 资产不存在，调用资产管理的资产注册lib
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
                        "source": "0",                     # 资产来源 "0": iep "1": 人工录入 "2":ptd "3":cmdb "4":防火墙
                        "create_person": server_uid,       # 创建人（此处为iep服务端设备id）
                        "use_person": "",                  # 使用人
                        "use_contact": "",                 # 使用人联系方式
                        "ip_addr": ip,                     # ip
                        "mac_addr": "",                    # mac
                        "group_id": "1"                    # 资产组标识  "1":内网资产 "2":外网资产
        }
        try:
            result = append_asset(propertykeys, json.loads(config["pg_conn"]))
            asset_id = json.loads(result)["asset_id"]
        except Exception as e:
            pg_conn.commit()
            response = "同步漏洞: 调用资产注册lib失败 -> error: %s" % str(e)
            return 'error', response
        logger.info("资产不存在 注册成功 -> asset_id: %s" % asset_id)
    else:
        asset_id = message
        logger.info("资产存在 -> asset_id: %s" % asset_id)
    # 进行资产UUID数据清洗
    tmpdict["client"]["uuid"] = asset_id
    # 处理完的漏洞数据写入新话题
    try:
        object = confluent_kafka.Producer({"bootstrap.servers": "%s:%s" % (config["kafka_ip"], config["kafka_port"])})
        sendData = json.dumps(tmpdict)
        object.produce("ACD_loophole_convert", sendData)
        flag = object.flush(timeout=5)
        if flag == 0:
            logger.info("写入漏洞话题数据成功")
    except Exception as e:
        elogger.error(str(e))
    pg_conn.commit()

    logger.info("漏洞数据同步完成")
    return 'success', response


'''
@description:    同步IEP端口数据   (postgresql 多线程数据同步)
@param {type}    pg_conn(object),  message(string),   config(dict),  logger(object)
@return:         'parse'(消息解析错误)/'error'(消息处理失败)/'success'(消息处理成功),  response(string)
'''
def insertPort(pg_conn, message, config, logger, elogger):
    logger.info('-' * 60)
    logger.info("开始同步端口数据")
    cur = pg_conn.cursor()
    response = ""
    try:
        tmpdict = json.loads(message)
        asset_id = tmpdict["client"]["uuid"]
        portdata = tmpdict["portinfos"]
        ip_addr = tmpdict["client"]["ip"]
        server_ip = tmpdict["client"]["server_ip"]
        if not config["limit_device"] == "-1" and not server_ip in config["limit_device"]:
            logger.warning("server_ip -> %s" % server_ip)
            pg_conn.commit()
            return 'success', response
    except:
        elogger.error('{message: %s }' % message)
        response = "同步端口: 数据格式解析错误"
        return 'parse', response
    
    # 获取资产管理中资产的uuid
    flag, asset_id = getAssetId(cur, ip_addr)
    if not flag:
        pg_conn.commit()
        return 'error', asset_id
    logger.info("资产 -> %s" % asset_id)
    sql = '''
            insert into h_asset_port(asset_id, protocol, s_ip, s_port, d_ip, d_port, process, r_time) values (%s, %s, %s, %s, %s, %s, %s, %s);
    '''
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for eachport in portdata:
        sip = eachport["sip"]
        dip = eachport["dip"]
        proto = str(eachport["proto"])
        proc = eachport["proc"]
        sport = eachport["sport"]
        dport = eachport["dport"]
        if int(sport) == -1:
            sport = None
        if int(dport) == -1:
            dport = None
        args = (asset_id, proto, sip, sport, dip, dport, proc, timestamp, )
        try:
            cur.execute(sql, args)
        except Exception as e:
            elogger.error(str(e))
            pg_conn.commit()
            response = "同步端口: 插入端口数据失败"
            return 'error', response

    pg_conn.commit()

    logger.info("端口数据同步完成")
    return 'success', response


'''
@description:    kafka生产者回调函数
@param {type}    err(object),  message(string)
@return:
'''
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


'''
@description:    kafka消费者处理数据函数
@param {type}    topic(string),  partition(int),  functions(object),  redis_conn(object),  pg_conn(object),  config(dict),  logger(object)
@return:
'''
def consumeData(topic, partition, functions, redis_conn, pg_conn, config, logger, elogger, dlogger):
    offsetkey = "%s_%s" % (topic, partition)
    redis_offset = redis_conn.get(offsetkey)
    broker_list = "%s:%s" % (config["kafka_ip"], config["kafka_port"])
    producer = confluent_kafka.Producer({"bootstrap.servers": broker_list})
    tp_c = TopicPartition(topic, partition, 0)
    consume = Consumer({
                        'bootstrap.servers': broker_list,
                        'group.id': config["kafka_group_id"],
                        'enable.auto.commit': False,
                        'max.poll.interval.ms': config["kafka_max_poll"],
                        'default.topic.config': {'auto.offset.reset': config["kafka_reset"]}
    })
    # 获取数据对应最小offset 与 redis记录中的offset比较
    kafka_offset = consume.get_watermark_offsets(tp_c)[0]
    if not redis_offset:
        offset = kafka_offset
    else:
        if int(redis_offset) > kafka_offset:
            offset = int(redis_offset)
        else:
            offset = kafka_offset
    # 重新绑定offset 消费
    tp_c = TopicPartition(topic, partition, offset)
    consume.assign([tp_c])
    data = consume.consume(config["length"], 5)
    write_offset = offset
    if data:
        dlogger.info("topic: %s  partition: %s\t  data_length : %s" % (topic, partition, len(data)))
        for eachmsg in data:
            if eachmsg.error():
                elogger.error('error: %s' % eachmsg.error())
                write_offset += 1
                continue
            # 处理日志数据函数  flag: 'parse'(消息解析错误)/'error'(消息处理失败)/'success'(消息处理成功)
            flag, message = functions[topic](pg_conn, eachmsg.value(), config, logger, elogger)
            write_offset += 1
            # 数据解析失败，需要往新话题写入数据
            if flag == 'parse':
                elogger.error(message)
                producer.produce('parse_error', eachmsg.value())
                producer.flush(timeout=1) #  0: 成功  1: 失败
            # 数据处理失败，需要往原话题写回数据
            elif flag == 'error':
                elogger.error(message)
                producer.produce(topic, eachmsg.value())
                producer.flush(timeout=1) #  0: 成功  1: 失败
            # 数据处理成功
            else:
                pass
            # with main_thread_lock:
            #     pass
    else:
        dlogger.info("topic: %s  partition: %s\t无数据" % (topic, partition))
    # 处理结束后， redis中更新offset
    tp_c = TopicPartition(topic, partition, write_offset)
    # 获取当前分区偏移量
    kafka_offset = consume.position([tp_c])[0].offset
    # 当前分区有消费的数据, 存在偏移量
    if kafka_offset >= 0:
        # 当redis维护的offset发成超限时，重置offset
        if write_offset > kafka_offset:
            write_offset = kafka_offset
    redis_conn.set(offsetkey, write_offset)
    consume.commit(offsets=[tp_c])


'''
@description:    kafka阻塞式消费数据
@param {type}    topic(string),  partition(int),  functions(object),  redis_conn(object),  pg_conn(object),  config(dict)
@return:
'''
def reset_offset(topic, partition, functions, redis_conn, pg_conn, config, elogger, dlogger):
    logger = record(filename='thread_service.log', process=topic, thread=partition)
    while True:
        try:
            consumeData(topic, partition, functions, redis_conn, pg_conn, config, logger, elogger, dlogger)
            time.sleep(3)
        except Exception as e:
            elogger.error("Error: consumeData function -> message: %s" % str(e))
            continue


'''
@description:    进程对应一个话题，进程开启多线程对应话题分区数量，同时消费数据
@param {type}    topic(string),  partition(int),  config(dict)
@return:
'''
def threatsConsume(topic, functions, config, ):
    logger = record(filename='process_service.log', process=topic)
    elogger = record(filename='error.log', process=topic)
    dlogger = record(filename='in_data.log', process=topic)
    logger.info('Run child process (%s)...' % (os.getpid()))
    # 子进程启动多线程方式消费当前分配的话题数据，线程数和分区数要匹配
    try:
        # 开启pg连接池
        pg_pool = PooledDB(
                            psycopg2,
                            1,
                            database=config["pg_db"],
                            user=config["pg_user"],
                            password=config["pg_passwd"], 
                            host=config["pg_host"],
                            port=config["pg_port"]
        )
    except psycopg2.OperationalError as e:
        elogger.error("Error: threatsConsume function pg connect fail-> message: %s" % str(e))
        sys.exit(0)
    pg_conn = pg_pool.connection()
    # 开启redis连接池
    redis_pool = redis.ConnectionPool(
                                        host=config["redis_ip"],
                                        port=config["redis_port"],
                                        db=config["redis_db"],
                                        password=config["redis_passwd"],
                                        decode_responses=True
    )
    redis_conn = redis.Redis(connection_pool=redis_pool)
    threads = []
    try:
        for partition in range(config["kafka_partitions"]):
            child_thread = threading.Thread(
                                            target=reset_offset,
                                            args=(
                                                    topic,
                                                    partition,
                                                    functions,
                                                    redis_conn,
                                                    pg_conn,
                                                    config,
                                                    elogger,
                                                    dlogger,
                                            ),
                                            name='LoopThread'
            )
            threads.append(child_thread)
        for eachthread in threads:
            eachthread.start()
        for eachthread in threads:
            eachthread.join()
        logger.warning("exit program with 0")
    except Exception as e:
        elogger.error("Error: threatsConsume function threads fail-> message: %s" % str(e))


'''
@description: 主程序
'''
def main():
    # 从Config.py中加载进程环境变量
    import Config; Config._init()
    config = Config.get_value()
    logger = record(filename='main_server.log')
    functions = {
                "ACD_organization": insertOrg,
                "ACD_client_info": insertAsset,
                "ACD_software_info": insertSoft,
                "ACD_hardware_info": insertHard,
                "ACD_host_service": insertServ,
                "ACD_host_proc": insertProc,
                "ACD_loophole_detect": insertHole,
                "ACD_host_ports": insertPort
    }
    topics = ["ACD_organization", "ACD_client_info", "ACD_software_info", "ACD_hardware_info", "ACD_host_service", "ACD_loophole_detect", "ACD_host_ports", "ACD_host_proc"]  # 待消费话题集合
    #topics = functions.keys()
    processes = []
    logger.info('Parent process %s.' % os.getpid())
    # 启动多进程方式同时消费所有话题
    for eachtopic in topics:
        p = Process(target=threatsConsume, args=(eachtopic, functions, config, ))
        logger.info('Child process will start.')
        processes.append(p)
    for eachprocess in processes:
        eachprocess.start()
    for eachprocess in processes:
        eachprocess.join()
    logger.info('Child process end.')


if __name__ == "__main__":
    main()

