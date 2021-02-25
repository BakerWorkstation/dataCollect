'''
@Author: sdc
@Date: 2019-12-30 11:38:49
LastEditTime: 2020-08-19 11:34:45
LastEditors: Please set LastEditors
@Description:  将origin_syslog中存储的syslog协议原始数据进行消费、解析处理
@FilePath: /opt/DataCollect/syslog/consumeKafka.py
'''
#!/usr/bin/env python3.6
# -*- coding:utf-8 -*-
# __author__: sdc


import os
import sys
import time
import json
import ujson
import base64
import asyncio
import datetime
import threading
import confluent_kafka
from multiprocessing import Process
from utils.handle import handle_data
from utils.hbaseApi import HbaseManul
from confluent_kafka import Consumer, KafkaError, TopicPartition

import psycopg2
from psycopg2.extras import RealDictCursor

main_thread_lock = threading.Lock()

# 从Config.py中加载进程环境变量
import Config; Config._init()
config_env = Config.get_value()

logger = config_env["logger"]
session_timeout = config_env["session_timeout"]
origin_syslog = config_env["origin_syslog"]
redis_pool = config_env["redis_pool"]
hbase_ip = config_env["hbase_ip"]
hbase_port = config_env["hbase_port"]
hbase_tube = config_env["hbase_tube"]
ppid = None


'''
@description:    kafka生产者回调函数
@param {type}    err(object),  message(string)
@return:
'''
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logger.error('Message delivery failed: {}'.format(err))
    else:
        logger.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


'''
@description:    批量写入hbase
@param {type}    logs(dict)
@return:
'''
def write2Hbase(logs):
    global ppid
    try:
        hBase = HbaseManul(hbase_ip, hbase_port)
    except Exception as e:
        logger.error('message: %s -> 主程序退出' % str(e))
        os.killpg(os.getpgid(ppid), 9)
    try:
        hBase.switchTable(hbase_tube)
        hBase.putDatas(logs)
    except Exception as e:
        logger.error('message: %s -> 主程序退出' % str(e))
        os.killpg(os.getpgid(ppid), 9)
    finally:
        hBase.close()

'''
@description:    origin_syslog话题数据处理函数
@param {type}    producer(object),  data(list),  write_offset(int)
@return:         write_offset(int)
'''
def handle_origin_syslog(producer, data, partition, write_offset, cursor):
    
    counter = 0
    logKey = {}
    hbaseDict = {}
    for eachmsg in data:
        #logger.info('-' * 50)
        #logger.info(time.time())
        if eachmsg.error():
            logger.error('data error: %s' % eachmsg.error())
            write_offset += 1
            continue
        # 处理日志数据函数
        flag, redisKey, rowkey, origin_log = handle_data(producer, eachmsg, config_env, cursor)
        # 打包每条原始日志，供批量写入hbase
        #logger.info(time.time())
        hbaseDict[rowkey] = {"info:message": ujson.dumps(origin_log)}
        if flag:
            counter += 1
        write_offset += 1
        # 合并每个redisKey内容， 合并后写入redis
        # 数据样例   redisKey = {'zhenguan': {'origin_count': 1, 'black_count': 1, 'timestamp': '2019-12-10 10:58:43', 'device_ip': '127.0.0.1'}}
        for key, value in redisKey.items():
            if not key in logKey:
                ip = value["device_ip"]
                logKey[key] = {ip: value}
                continue
            if not value["device_ip"] in logKey[key]:
                ip = value["device_ip"]
                logKey[key][ip] = value
            else:
                ip = value["device_ip"]
                logKey[key][ip]["origin_count"] += value["origin_count"]
                logKey[key][ip]["black_count"] += value["black_count"]
                logKey[key][ip]["timestamp"] = value["timestamp"]
        if partition == 0:
            pass
            #print('logkey: %s' % logKey)
            #print('redisKey: %s' % redisKey)
    # 批量往hbase记录原始日志
    #write2Hbase(hbaseDict)
    #if partition == 0:
    #print('total : %s\t logkey: %s' % (len(data), logKey))
    # 批量往kafka生产数据
    if counter:
        logger.warning("partition: %s\toffset: %s\ttotal data length : %s\tproduce data length : %s" % (partition, write_offset, len(data), counter))
        producer.flush(timeout=3) #  0: 成功  1: 失败
    # redis写入计数
    #  logKey = {'zhenguan': {'127.0.0.1': {'origin_count': 222, 'black_count': 222, 'timestamp': '2019-12-10 13:15:29'}}}
    with main_thread_lock:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
        origin_key = "firewall_origin_%s" % timestamp
        origin_total = "firewall_origin_total"
        black_key = "firewall_black_%s" % timestamp
        black_total = "firewall_black_total"
        try:
            # 往redis同步防火墙当天接收原始日志数量
            count = redis_pool.get(origin_key)
            if count:
                count = int(count) + len(data)
                redis_pool.set(origin_key, count)
            else:
                redis_pool.set(origin_key, len(data))
            # 往redis同步防火墙接收原始日志总数量
            count = redis_pool.get(origin_total)
            if count:
                count = int(count) + len(data)
                redis_pool.set(origin_total, count)
            else:
                redis_pool.set(origin_total, len(data))
            # 把logKey数据同步到redis
            for key, value in logKey.items():
                # 同步厂商原始数量到redis
                manuf_origin = '%s_origin_%s' % (key, timestamp)
                manuf_black = '%s_black_%s' % (key, timestamp)
                manuf_origin_sum = '%s_origin_sum' % key
                manuf_black_sum = '%s_black_sum' % key
                # 遍历所有设备（ip为key）
                for ip, record in value.items():
                    # 同步厂商当天原始数据
                    count = redis_pool.get(manuf_origin)
                    if count:
                        count = int(count) + record["origin_count"]
                        redis_pool.set(manuf_origin, count)
                    else:
                        redis_pool.set(manuf_origin, record["origin_count"])
                    # 同步厂商原始总数据
                    count = redis_pool.get(manuf_origin_sum)
                    if count:
                        count = int(count) + record["origin_count"]
                        redis_pool.set(manuf_origin_sum, count)
                    else:
                        redis_pool.set(manuf_origin_sum, record["origin_count"])
                    # 同步厂商当天黑数据
                    count = redis_pool.get(manuf_black)
                    if count:
                        count = int(count) + record["black_count"]
                        redis_pool.set(manuf_black, count)
                    else:
                        redis_pool.set(manuf_black, record["black_count"])
                    # 同步厂商黑总数据
                    count = redis_pool.get(manuf_black_sum)
                    if count:
                        count = int(count) + record["black_count"]
                        redis_pool.set(manuf_black_sum, count)
                    else:
                        redis_pool.set(manuf_black_sum, record["black_count"])
                    # 同步防火墙当天黑数据
                    count = redis_pool.get(black_key)
                    if count:
                        count = int(count) + record["black_count"]
                        redis_pool.set(black_key, count)
                    else:
                        redis_pool.set(black_key, record["black_count"])
                    # 同步防火墙黑总数据
                    count = redis_pool.get(black_total)
                    if count:
                        count = int(count) + record["black_count"]
                        redis_pool.set(black_total, count)
                    else:
                        redis_pool.set(black_total, record["black_count"])
                    # 同步安全设备最后发现时间到redis
                    device = redis_pool.get(key)
                    if not device:
                        deviceDict = {ip: {"device_ip": ip, "first_send_time": record["timestamp"], "last_send_time": record["timestamp"]}}
                        redis_pool.set(key, json.dumps(deviceDict))
                    else:
                        deviceDict = json.loads(device)
                        if ip in deviceDict:
                            deviceDict[ip]["last_send_time"] = record["timestamp"]
                            redis_pool.set(key, json.dumps(deviceDict))
                        else:
                            deviceDict[ip] = {"device_ip": ip, "first_send_time": record["timestamp"], "last_send_time": record["timestamp"]}
                            redis_pool.set(key, json.dumps(deviceDict))
        except Exception as e:
            logger.error('error: redis计数失败 -> message: %s' % str(e))
    return write_offset


'''
@description:    kafka消费者处理数据函数
@param {type}    topic(string),  partition(int)
@return:
'''
def consumeData(topic, partition, functions, cursor):
    offsetkey = "%s_%s" % (topic, partition)
    redis_offset = redis_pool.get(offsetkey)
    broker_list = "%s:%s" % (config_env["kafka_ip"], config_env["kafka_port"])
    producer = confluent_kafka.Producer({"bootstrap.servers": broker_list})
    tp_c = TopicPartition(topic, partition, 0)
    consume = Consumer({
                        'bootstrap.servers': broker_list,
                        'group.id': config_env["group_id"],
                        'enable.auto.commit': False,
                        'max.poll.interval.ms': config_env["kafka_max_poll"],
                        'default.topic.config': {'auto.offset.reset': config_env["kafka_reset"]}
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
    data = consume.consume(config_env["length"], 1)
    write_offset = offset
    if data:
        logger.info("topic: %s\tpartition: %s\t data length : %s" % (topic, partition, len(data)))
        write_offset = functions[topic](producer, data, partition, write_offset, cursor)
    else:
        logger.info("topic: %s\tpartition: %s\t无数据" % (topic, partition))
    # 处理结束后， redis中更新offset
    tp_c = TopicPartition(topic, partition, write_offset)
    # 获取当前分区偏移量
    kafka_offset = consume.position([tp_c])[0].offset
    # 当前分区有消费的数据, 存在偏移量
    if kafka_offset >= 0:
        # 当redis维护的offset发成超限时，重置offset
        if write_offset > kafka_offset:
            write_offset = kafka_offset
    redis_pool.set(offsetkey, write_offset)
    consume.commit(offsets=[tp_c])


'''
@description:    kafka阻塞式消费数据
@param {type}    topic(string),  partition(int)
@return:
'''
def reset_offset(topic, partition, functions):
    while True:
        try:
            conn = psycopg2.connect(**config_env["pg_conn"])
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            consumeData(topic, partition, functions, cursor)
            conn.commit()
            conn.close()
            #time.sleep(0.1)
        except Exception as e:
            logger.error("Error: consumeData function -> message: %s" % str(e))
            continue


'''
@description:    进程对应一个话题，进程开启多线程对应话题分区数量，同时消费数据
@param {type}    topic(string),  partition(int),  config(dict)
@return:
'''
def threatsConsume(topic, functions):
    logger.info('Run child process %s (%s)...' % (topic, os.getpid()))
    # 子进程启动多线程方式消费当前分配的话题数据，线程数和分区数要匹配
    threads = []
    try:
        for partition in range(config_env["partitions"]):
            child_thread = threading.Thread(
                                            target=reset_offset, 
                                            args=(
                                                    topic, 
                                                    partition, 
                                                    functions,
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
        logger.error("Error: threatsConsume function threads fail-> message: %s" % str(e))


'''
@description: 主程序
'''
def main():
    topics = ['origin_syslog']  # 待消费话题集合
    processes = []
    logger.info('Parent process %s.' % os.getpid())
    functions = {
                 'origin_syslog': handle_origin_syslog
    }
    # 启动多进程方式同时消费所有话题
    global ppid
    ppid = os.getpid()
    for eachtopic in topics:
        p = Process(target=threatsConsume, args=(eachtopic, functions, ))
        logger.info('Child process will start.')
        processes.append(p)
    for eachprocess in processes:
        eachprocess.start()
    for eachprocess in processes:
        eachprocess.join()
    logger.info('Child process end.')


if __name__ == "__main__":
    main()
