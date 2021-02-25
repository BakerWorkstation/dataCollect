'''
@Author: your name
@Date: 2020-06-16 14:25:59
@LastEditTime: 2020-06-16 14:26:00
@LastEditors: your name
@Description: In User Settings Edit
@FilePath: /opt/DataCollect/kafkaTopics.py
'''
#!/usr/bin/env python3.6
# -*- coding:utf-8 -*-
# __author__: sdc

import sys
import configparser
from confluent_kafka import admin

global_conf = configparser.ConfigParser()
global_conf.read("/opt/DataCollect/global.conf")
#kafka_ip = global_conf.get("KAFKA", "ip")
#kafka_port = int(global_conf.get("KAFKA", "port"))
kafka_ip = "10.255.175.92"
kafka_port = 6667

#kafka_ip = "10.255.175.109"
#kafka_port = 9092

partitions = int(global_conf.get("KAFKA", "partitions"))
#conf = configparser.ConfigParser()
#conf.read("/opt/DataCollect/syslog/conf/server.conf")
#origin_syslog = conf.get("KAFKA", "origin_syslog")
#handle_syslog = conf.get("KAFKA", "handle_syslog")


#topics = [origin_syslog, handle_syslog]
topics = ["origin_syslog", "syslog", "ACD_heartbeat", "ACD_organization", "ACD_client_info", "ACD_software_info", "ACD_hardware_info", "ACD_host_service", "ACD_loophole_detect", "ACD_loophole_convert", "ACD_host_proc","PTD_BlackData_Processed3", "ACD_eventlog"]
#topics = ["Standardization_BlackData", "Standardization_WhiteData", "black_log_converted", "white_log_converted"]

object = admin.AdminClient({"bootstrap.servers": "%s:%s" % (kafka_ip, kafka_port)})
new_topics = [admin.NewTopic(topic, num_partitions=partitions, replication_factor=1) for topic in topics]
fs = object.create_topics(new_topics)
#print(fs)
for topic, f in fs.items():
    #print(f)
    try:
        f.result()  # The result itself is None
        # print f.result(),"hehe"
        print("Topic {} created".format(topic))
        sys.exit(0)
    except Exception as e:
        print("Failed to create topic {}: {}".format(topic, e))

new_topics = [admin.NewPartitions(topic, new_total_count=partitions) for topic in topics]
fs = object.create_partitions(new_topics)

#print(fs)
for topic, f in fs.items():
    #print(f)
    try:
        f.result()  # The result itself is None
        # print f.result(),"hehe"
        print("Topic {} update partitions ".format(topic))
    except Exception as e:
        print("Failed to update partitions {}: {}".format(topic, e))
