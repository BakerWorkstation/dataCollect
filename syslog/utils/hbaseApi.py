'''
@Author: your name
@Date: 2020-05-13 14:23:41
LastEditTime: 2020-08-19 10:30:40
LastEditors: Please set LastEditors
@Description: In User Settings Edit
@FilePath: /opt/DataCollect/syslog/utils/hbaseApi.py
'''
#!/usr/bin/env python3.6
# -*- coding:utf-8 -*-
# __author__: sdc

# 需要安装如下模块
# pip install happybase
# pip install thrift




import json
import happybase

class HbaseManul(object):

    def __init__(self, ip, port):
        self.conn = happybase.Connection(host=ip, port=port, protocol='compact',transport='framed')

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
        # put(row, data, timestamp=None, wal=True)---> 插入数据，无返回值
        #   row---> 行，插入数据的时候需要指定；
        #   data---> 数据，dict类型，{key:value}构成，列与值均为str类型
        #   timestamp--->时间戳，默认None, 即写入当前时间戳
        #   wal---> 是否写入wal, 默认为True
        # rowkey: test1, data: {"info:data":"test hbase"}
        self.table.put(rowkey, data)

    # 批量插入数据
    def putDatas(self, datas):
        bat = self.table.batch()
        for rowkey, message in datas.items():
            bat.put(rowkey, message)
        bat.send()

    # 查询数据
    def getData(self, rowkey):
        return self.table.row(rowkey)

    # 遍历表
    def scanTable(self):
        for key, value in self.table.scan():   # scan参数 范围查询 row_start='test1',row_stop='test3' 或者 指定查询的row  row_prefix='test3'
            print("key: %s,  value: %s" % (key, value))

    # 删除行
    def deleteRow(self, rowkey):
        self.table.delete(rowkey)

    # 删除表
    def deleteTable(self, tableName):
        self.conn.delete_table(tableName, True)

    def close(self):
        self.conn.close()

    
def main():
    ip = "10.255.175.92"
    port = 9090
    hBase = HbaseManul(ip, port)
    #print(hBase.allTables())
    # 创建表 t1，指定列簇名"info"
    #tableName = "PTD_Source_BlackData"
    #tableName = "iep_event"
    tableName = "SYSLOG"
    # data = {"info": {}}
    # hBase.createTable(tableName, data)

    # # 连接表
    hBase.switchTable(tableName)
    #hBase.scanTable()
    #hBase.deleteRow("test3")
    #hBase.scanTable()
    # 插入数据, rowkey: test1, data: {"info:data":"test hbase"}
    # rowkey = "test3"
    # data = {"info:data":"test hbase"}
    # hBase.putData(rowkey, data)
    #hBase.scanTable()
    # 获取数据
    rowkey = "o@OT_60030191800202_syslog"
    a = hBase.getData(rowkey)
    # b = a[b'info:content'].decode()
    # c = {"data": [json.loads(b)]}
    print(a)
    # import confluent_kafka
    # kafka_ip = "10.255.175.92"
    # kafka_port = 6667
    # topic = "PTD_BlackData_Processed3"

    # producer = confluent_kafka.Producer({"bootstrap.servers": "%s:%s" % (kafka_ip, kafka_port)})
    # producer.produce(topic, json.dumps(c))
    # flag = producer.flush()
    # print(flag)

    


    # # 删除表
    # hBase.deleteTable(tableName)

if __name__ == '__main__':
    main()
