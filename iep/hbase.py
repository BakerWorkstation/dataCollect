# -*- coding:UTF-8 -*-

import os,re
import json
import configparser
import datetime
import random
from confluent_kafka import Consumer, KafkaError, Producer, KafkaException, TopicPartition, Message, admin
import happybase
import psycopg2,time
from multiprocessing import Pool


# 从Config.py中加载进程环境变量

#hbase api
class Hbaseapi(object):
    def __init__(self, ip, port):
        self.conn = happybase.Connection(host=ip, port=port, protocol='compact', transport='framed')
    def random_str(self,data):
        seed = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        sa = []
        for i in range(4):
          sa.append(random.choice(seed))
        sa.append(data)
        salt = ''.join(sa)
        return salt
    # 获取所有表
    def allTables(self):
        return self.conn.tables()

    # 创建表 t1，指定列簇名"info"
    def createTable(self, tableName, data):
        self.conn.create_table(tableName, data)

    # 连接表
    def switchTable(self, tableName):
        self.table = self.conn.table(tableName)

    # 插入数据
    def putData(self, rowkey, data):
        return self.table.put(rowkey, data)

    # 查询数据
    def getData(self, rowkey):
        return self.table.row(rowkey)
    def getDatas(self,rowkeys):
        rows = self.table.rows(rowkeys)
        #print rows
        #print type(rows)
        for key, data in rows:
            print(key, data)
    #查看指定行列所有版本  table.cells(b'row-key', b'cf1:col1', versions=2)
    #批量上传处理函数

    def send_bluk(self,datas,row):
        b = self.table.batch()
        for i,j in datas.items():
            b.put(i,{row:j})
        b.send()
    def scanTable(self):
        datas={}
        for key, value in self.table.scan():   # scan参数 范围查询 row_start='test1',row_stop='test3' 或者 指定查询的row  row_prefix='test3'
            datas[key] = value
        return datas

    # 删除行
    def deleteRow(self, rowkey):
        self.table.delete(rowkey)

    # 删除表
    def deleteTable(self, tableName):
        self.conn.delete_table(tableName, True)

    def open_con(self):
        self.conn.open()
    def close_con(self):
        self.conn.close()
