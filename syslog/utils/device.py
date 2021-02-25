'''
@Author: sdc
@Date: 2019-12-24 13:53:53
@LastEditTime: 2020-06-12 16:25:16
@LastEditors: Please set LastEditors
@Description:  采集发现的设备(已存redis) -> 录入到安全设备管理表中
@FilePath: /opt/DataCollect/syslog/utils/device.py
'''
#!/usr/bin/env python3.6
# -*- coding:utf-8 -*-
# __author__: sdc

import sys
import json
import time
import redis
import datetime
import psycopg2
import requests
import configparser
from DBUtils.PooledDB import PooledDB


'''
@description:  从redis取数据 -> 同步到pg库
@param    pg_conn(object),  redis_conn(object), deviceType(dict),  logStamp(string)
@return:   True/False
'''
def syncdevice(pg_conn, redis_conn, deviceType, logStamp, heart_url, heart_logtime, heart_interfacetime):
    print('%s\t安全设备 -> 开始同步' % logStamp)
    interfaceSet = set()
    log_dict = {}
    response = False
    cur = pg_conn.cursor()
    getsql = 'select device_type_id from k_device_type where device_type=%s; '

    typesql = '''with w1 as(select ('x'||substr(md5(%s),1,16))::bit(64)::bigint as tra_id),    
                    upsert as (update k_device_type set r_time=%s where device_type=%s returning *)    
                        insert into k_device_type(device_type, r_person, r_time) select %s, %s, %s from w1   
                      where pg_try_advisory_xact_lock(tra_id) and not exists (select 1 from upsert where device_type=%s); '''
    
    devicesql = '''with w1 as(select ('x'||substr(md5(%s),1,16))::bit(64)::bigint as tra_id),    
                    upsert as (update h_device_info set device_ip=%s, r_time=%s where device_id=%s and device_type_id=%s returning *)    
                        insert into h_device_info(device_id, node_id, device_type_id, device_name, device_ip, state, r_person, r_time) select %s, %s, %s, %s, %s, %s, %s, %s from w1   
                      where pg_try_advisory_xact_lock(tra_id) and not exists (select 1 from upsert where device_id=%s); '''
    
    for device, name in deviceType.items():
        logSet = set()
        log_dict[device] = logSet
        try:
            deviceInfo = json.loads(redis_conn.get(device))
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            #print("%s\terror: 解析redis中value失败 message: %s" % (logStamp, str(e)))
            continue
        # 插入安全设备类型表
        typeargs = (name, timestamp, name, name, 'admin', timestamp, name, )
        flag = cur.execute(typesql, typeargs)
        if flag:
            print('%s\terror: 插入安全设备类型表失败， message: %s' % (logStamp, str(flag)))
            return response
        # 取出类型ID
        getargs=(name, )
        flag = cur.execute(getsql, getargs)
        if flag:
            print('%s\terror: 查询安全设备类型表失败， message: %s' % (logStamp, str(flag)))
            return response
        result = cur.fetchone()
        if result:
            typeId = result[0]
        else:
            print('%s\terror: 安全设备类型表，类型为空 -> 类型名称: %s' % (logStamp, name))
            return response
        # 插入安全设备详情表
        for deviceId, info in deviceInfo.items():
            ip = info["device_ip"]
            #first_time = info["first_send_time"]
            last_time = info["last_send_time"]
            timeArray = time.strptime(last_time, "%Y-%m-%d %H:%M:%S")
            last_time = int(time.mktime(timeArray))
            if last_time + heart_logtime * 60 > int(time.time()):
                logSet.add(deviceId)
            deviceargs = (deviceId, ip, timestamp, deviceId, typeId, deviceId, '1', typeId, name, ip, '1', 'admin', timestamp, deviceId, )
            flag = cur.execute(devicesql, deviceargs)
            if flag:
                print('%s\terror: 插入安全设备详情表失败， message: %s' % (logStamp, str(flag)))
                return response        
    for eachdevice in ['iep', 'ptd']:
        try:
            response = requests.get(heart_url + '%s/list' % eachdevice, verify=False)
            interfaceSet = interfaceSet | set(json.loads(response.text).keys())
            for eachD in json.loads(response.text).keys():
                redis_conn.zadd("%s_heart" % eachdevice, {eachD: int(time.time())})
        except Exception as e:
            print(str(e))
            pass
        online = redis_conn.zrangebyscore("%s_heart" % eachdevice, int(time.time()) - heart_interfacetime * 60, int(time.time()), withscores=True)
        if online:
            for eachline in online:
                log_dict[eachdevice].add(eachline[0])
    for key, value in log_dict.items():
        log_dict[key] = list(value)
    print(log_dict)
    redis_conn.set("online", json.dumps(log_dict))
    pg_conn.commit()
    response = True
    return response


'''
@description: 主函数
@param    logStamp(string)
'''
def main(logStamp):
    try:
        global_conf = configparser.ConfigParser()
        global_conf.read("/opt/DataCollect/global.conf")
        redis_ip = global_conf.get("REDIS", "ip")
        redis_port = int(global_conf.get("REDIS", "port"))
        redis_passwd = global_conf.get("REDIS", "password")
        pg_conf = json.loads(global_conf.get("PG", "pg_conn"))
        deviceType = json.loads(global_conf.get("DEVICE", "deviceType"))
        heart_url = global_conf.get("HEART", "url")
        heart_logtime = int(global_conf.get("HEART", "logtime"))
        heart_interfacetime = int(global_conf.get("HEART", "interfacetime"))
        conf = configparser.ConfigParser()
        conf.read("/opt/DataCollect/syslog/conf/server.conf")
        redis_db = int(conf.get("REDIS", "db"))
    except Exception as e:
        print('%s\terror: 配置文件解析错误, message: %s' % (logStamp, str(e)))
        sys.exit(0)
    try:
        pg_pool = PooledDB(
                            psycopg2, 
                            1,
                            database=pg_conf["database"],
                            user=pg_conf["user"],
                            password=pg_conf["password"], 
                            host=pg_conf["host"],
                            port=pg_conf["port"]
        )
    except psycopg2.OperationalError as e: 
        print(str(e))
        sys.exit(0)
    pg_conn = pg_pool.connection()
    redis_pool = redis.ConnectionPool(
                                        host=redis_ip,
                                        port=redis_port,
                                        db=redis_db,
                                        password=redis_passwd,
                                        decode_responses=True
    )
    redis_conn = redis.Redis(connection_pool=redis_pool)
    flag = syncdevice(pg_conn, redis_conn, deviceType, logStamp, heart_url, heart_logtime, heart_interfacetime)
    if flag:
        print('%s\t安全设备 -> 同步成功' % logStamp)
    else:
        print('%s\t安全设备 -> 同步失败' % logStamp)

    flag = os_ver(pg_conn, logStamp)
    if flag:
        print('%s\t操作系统 -> 同步成功' % logStamp)
    else:
        print('%s\t操作系统 -> 同步失败' % logStamp)

def os_ver(pg_conn, logStamp):
    print('%s\t操作系统 -> 开始同步' % logStamp)
    response = False
    cur = pg_conn.cursor()
    sql = '''
             with cte as (select distinct os_ver, case when os_ver ~ '^Win' then substring(os_ver from '^Win[0-9A-z]*')
    when os_ver ~ '^Ubuntu' then substring(os_ver from '^Ubuntu\s[0-9]*.[0-9]*')
 when os_ver ~ '^CentOS' then 'CentOS' || substring(os_ver from '\s[0-9]\d*')
 end  as os_ver1  from h_hardware_asset_info)

 update h_hardware_asset_info ainfo set os_group=cte.os_ver1 from cte where ainfo.os_ver = cte.os_ver;
    '''
    flag = cur.execute(sql)
    if flag:
        return response
    pg_conn.commit()
    response = True
    return response



if __name__ == "__main__":
    while 1:
        # 日志时间戳
        print('-' * 50)
        logStamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        main(logStamp)
        print('%s\t休眠 -> 300秒' % logStamp)
        time.sleep(300)
