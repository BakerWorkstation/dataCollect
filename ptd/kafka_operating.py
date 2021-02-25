# -*- coding:UTF-8 -*-
from confluent_kafka import Producer, Consumer, KafkaError
import time
import json
import configparser

# 从Config.py中加载进程环境变量
import Config; Config._init()
config_env = Config.get_value()

kafka_ip = config_env["kafka_ip"]
kafka_port = config_env["kafka_port"]

# conf = configparser.ConfigParser()
# fileUrl = "./server.conf"
# conf.read(fileUrl)
# save_kafka_ip = conf.get("save_kafka", "ip")
# save_kafka_port = conf.get("save_kafka", "port")
# read_kafka_ip = conf.get("read_kafka", "ip")
# read_kafka_port = conf.get("read_kafka", "port")

save_kafka_ip = kafka_ip
save_kafka_port = kafka_port
read_kafka_ip = kafka_ip
read_kafka_port = kafka_port

logger = config_env["logger_kafka"]

def add_to_kafka(data,topic):

    ##producer配置，dict格式
    p = Producer({'bootstrap.servers': save_kafka_ip+":"+str(save_kafka_port)})

    ##回调函数
    def delivery_report(err, msg):
        if err is not None:
            logger.error('Message delivery failed: {}'.format(err))
            # print('Message delivery failed: {}'.format(err))
        else:
            # print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
            pass

    ##发送
    # p.produce(topic, data, callback=delivery_report)
    # # p.produce(topic, json.dumps(data).encode('utf-8'), callback=delivery_report)
    # # p.produce(topic, json.dumps(data,ensure_ascii=False), callback=delivery_report)
    # # p.poll(5)  ##等待返回结果最大时常，单位秒
    # p.flush()


    #----------------------性能调优----------------------
    ##发送
    for data_i in data:
        p.produce(topic, data_i, callback=delivery_report)

    x = p.flush(10)
    if x != 0:
        logger.warning("----------kafka存入数据过程中存在积压--------------")


def read_from_kafka(topic,group_id,reset):
    c = Consumer({
        'bootstrap.servers': read_kafka_ip+":"+str(read_kafka_port),
        'group.id': group_id,
        'default.topic.config': {
            'auto.offset.reset': reset   # largest, smallest, earliest
    }
    })

    c.subscribe([topic])


    def function(data):
        return data
        
    while True:
        msg = c.poll(1)   # c.poll(1)等待返回结果最大时长 单位秒

        if msg is None:
            # logger.info('topic中数据已全部消费完')
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logger.error(msg.error())
                # print(msg.error())
                break
        # elif msg is None:
        #     break
        #print('Received message: {}'.format(msg.value().decode('utf-8')))


        # a = msg.value().decode('utf-8')
        # function(a)
        yield msg.value().decode('utf-8')


        # return values_list.append(a)
        # print values_list

    c.close()
