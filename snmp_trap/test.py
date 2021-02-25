#! /usr/bin/env python3
# -*- coding:utf-8 -*-


import os
import sys
import json
import confluent_kafka

def get_trap_info(ip, port, log_file):
    kafka_ip = ip
    kafka_port = port
    log_obj = open(log_file,'a')
    sum = ''
    for sysline in sys.stdin:
        sum += sysline
    log_obj.write(sum)
    log_obj.close()
    try:
        object = confluent_kafka.Producer({"bootstrap.servers": "%s:%s" % (kafka_ip, kafka_port)})
        sendData = {"message": sum}
        object.produce("snmp_trap", json.dumps(sendData), "snmp_trap")
        object.flush()
    except Exception as e:
        print(str(e))

if __name__ =='__main__':
    if len(sys.argv) <4:
        print("The param is not correct")
        exit()

    ip = sys.argv[1]
    port = sys.argv[2]
    log_file = sys.argv[3]
    get_trap_info(ip, port, log_file)
