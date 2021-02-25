#!/usr/bin/env python3.6
# -*- coding:utf-8 -*-
# __author__: sdc

import time
import uuid
import datetime
import ConfigParser
import confluent_kafka

try:
    iniFileUrl = "/opt/DataCollect/global.conf"
    globalconf = ConfigParser.ConfigParser()
    globalconf.read(iniFileUrl)
    iniFileUrl = "/opt/DataCollect/syslog/conf/config.conf"
    conf = ConfigParser.ConfigParser()
    conf.read(iniFileUrl)
except Exception as e:
    logging.error(str(e))
    sys.exit()

def confluent_kafka_consumer_performance(topic, reset):
    global conf
    kafka_ip = globalconf.get('KAFKA', 'ip')
    kafka_port = globalconf.get('KAFKA', 'port')
    kafka_conf = {
             'bootstrap.servers': '%s:%s' % (kafka_ip, kafka_port),
             'group.id': str(uuid.uuid1()).replace("-", ""),
             'session.timeout.ms': 6000,
             'default.topic.config': {
                                       'auto.offset.reset': reset     # largest , smallest, earliest
            }
    }
    ff = open("./%s.txt" % topic, "w")
    consumer = confluent_kafka.Consumer(**kafka_conf)
    consumer.subscribe([topic])
    sum = 0
    count = 0
    while True:
        msg = consumer.poll(1)
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if msg:
            try:
                message = msg.value()
                if not str(message) in 'Broker: No more messages':
                    #print(message)
                    ff.write("%s\t%s\n" % (timestamp, message))
                    sum += 1
            except:
                pass
        else:
            if count > 2:
                break
            else:
                count += 1
                time.sleep(1)
    ff.write("%s\ttotal: %s\n" % (timestamp, sum))
    ff.close()
    consumer.close()

def main():
    reset = 'smallest'
    origin_topic = conf.get('KAFKA', 'origin_syslog')
    handle_topic = conf.get('KAFKA', 'handle_syslog')
    confluent_kafka_consumer_performance(origin_topic, reset)
    confluent_kafka_consumer_performance(handle_topic, reset)

if __name__ == '__main__':
    main()
