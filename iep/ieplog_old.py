#!/usr/bin/env python3.6
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


from datademo import Datademo
from hbase import Hbaseapi

# 从Config.py中加载进程环境变量
import Config; Config._init()
config_env = Config.get_value()

logger = config_env["logger"]
redis_pool = config_env["redis_pool"]

#kafka api 暂时不写redis缓存offset的方案
class Kafkaapi_redis(object):
    def __init__(self,ip_port,groupid,timeout,max_bytes):
        self.kafka_consumer_conf={
        'bootstrap.servers':ip_port,
        'group.id':groupid,
        'session.timeout.ms':timeout,
        'fetch.message.max.bytes':max_bytes,
        'default.topic.config':{'auto.offset.reset':"smallest"}
    }
        self.kafka_pro_conf={
        'bootstrap.servers': config_env["kafka_conn"]
    }
        self.kafka_pro = Producer(self.kafka_pro_conf)
        self.kafka_cum = Consumer(self.kafka_consumer_conf)
        self.kafka_cum_list = []
        self.white_data=[]
        self.black_data=[]
        
    #回调函数
    def delivery_report(self,err, msg):
        if err is not None:
            print('Message delivery failed: {}'.format(err))#输出错误信息
            if msg.topic() == "iep_eventlog_white":
                self.white_data.append(msg.value().decode("utf-8"))
            else:
                self.black_data.append(msg.value().decode("utf-8"))
        else:
            pass
    #生产数据
    def pro_data(self,topic,datas):
        try:
            for data in datas:
                self.kafka_pro.produce(topic,data.encode('utf-8'),callback=self.delivery_report)
            return "ok"
        except Exception as e:
            print(str(e))
            return "error"

    # 手动刷新调用返回值
    def pro_flush(self,time):
        try:
            xx=self.kafka_pro.flush(time)
            if self.white_data == []:
                pass
            else:
                self.save_error_data(datatype="white")
                self.white_data = []
            if self.black_data == []:
                pass
            else:
                self.save_error_data(datatype="black")
                self.black_data =[]
            return xx
        except Exception as e:
            print(str(e))
            return -1
            
    #保存错误数据到本地
    def save_error_data(self,datatype,filepath="/var/log/datacollect/"):
        try:
            with open(filepath+str(time.time())+datatype,"w+") as f:
                json.dump(self.black_data,f)
        except Exception as e:
            print(str(e))
            
    #自动订阅话题消费 
    def com_data(self,topic,num):
        self.kafka_cum.subscribe([topic])
        datas = self.kafka_cum.consume(num,1)
        return datas

    # 返回话题分区数 为每个分区制定一个消费者
    def detail_topic(self,topicname):
        object=admin.AdminClient(self.kafka_pro_conf)
        try:
            data=object.list_topics(timeout=10)
            #print(data)
            ##########
            print(data.topics[topicname])
            partitions_num = len(data.topics[topicname].partitions)
            print(partitions_num)
            if len(self.kafka_cum_list) ==0:
                for i in range(partitions_num):
                    self.kafka_cum_list.append(Consumer(self.kafka_consumer_conf))
                #for i in range(partitions_num):
                #    B=TopicPartition(topicname,partition=i,offset=0)
                #    self.kafka_cum_list[i].assign([B])
            #########
            return True,partitions_num
        except KeyError as ex:
            print(str(ex)+"没有这个话题")
            return False,str(ex)
        except Exception as ex:
            print(str(ex))
            return False,str(ex)
    #初始化所有的kafkalist中的offset
    def kafka_cum_list_init(self,partitions_dict,topicname):
        for i in range(len(partitions_dict)):
            B=TopicPartition(topicname,partition=i,offset=partitions_dict[i][0])
            self.kafka_cum_list[i].assign([B])

    #重置kafkalist中某个分区offset
    def kafka_list_assign_reset(self,partition_number,offset_number,topic_name):
        self.kafka_cum_list[i] = Consumer(self.kafka_consumer_conf)
        self.kafka_cum_list[i].assign([B])
        
    #返回所有分区的offset详情
    def partitions_offsetrd(self,topic_name):
        #print(topic_name)
        settopic_flag,partitions_num = self.detail_topic(topic_name)
        #print(settopic_flag,partitions_num)
        if settopic_flag == False:
            pass
            return False,partitions_num
        else:
            partitions_dict={}
            for i in range(partitions_num):
                partitions_dict[i] = self.kafka_cum.get_watermark_offsets(TopicPartition(topic=topic_name,partition=i,offset=0))
            #print(partitions_dict)
            return True,partitions_dict
                
    #制定分区 offset消费
    def com_data_byoffset(self,num,partition_num):
        #self.kafka_cum.subscribe([topic])
        #B=TopicPartition(topic,partition=partition_num,offset=offset_num)
        #self.kafka_cum.assign([B])
        datas = self.kafka_cum_list[partition_num].consume(num,1)
        #print(len(datas))
        return datas
    def seek_partition_offset(self,topicname,partition_number,offset_number):
        self.kafka_cum_list[partition_number].seek(TopicPartition(topicname,partition=partition_number,offset=offset_number))
    #获取话题列表
    def get_topic(self):
        self.kafka_admin = admin.AdminClient(kafka_pro_conf)
        return self.kafka_admin.list_topics(timeout=1)

    #重新连接kafka生产者
    def reset_pro(self):
        self.kafka_pro = Producer(self.kafka_pro_conf)



        #获取资产信息
def get_asset(data,cur):
    sql="""select a.os_ver,string_agg(e.software_name,','),f.ip_addr,f.mac_addr,a.asset_level,a.create_person,a.manage_person,b.node_name,c.type_name,d.group_name,a.asset_id \
        from h_hardware_asset_info as a \
        left join sys_node_info as b on a.node_id=b.node_id \
        left join h_asset_type as c on a.type_id=c.type_id \
        left join h_software_info as e on e.asset_id=a.asset_id \
        left join h_asset_ip_info as f on f.asset_id=a.asset_id \
        left join h_asset_group as d on f.group_id=d.group_id \
        where a.asset_id in (select unnest(%s)) \
        group by a.asset_id,b.node_name,c.type_name,d.group_name,f.ip_addr,f.mac_addr;"""
    cur.execute(sql,(data,))
    rows = cur.fetchall()
    return rows

    #测试kafka和资产的接口
def input_kafka_output_hbase(id,sum_number):
    hb = Hbaseapi(config_env["hbase_ip"], config_env["hbase_port"])
    kfk = Kafkaapi_redis(config_env["kafka_conn"], config_env["group_id"], str(config_env["timeout"]), config_env["max_bytes"])
    flag,partitions_dict = kfk.partitions_offsetrd("ACD_eventlogtest1")
    if flag != True:
        logger.error("查询话题失败   :"+str(partitions_dict))
    else:
        partitions_len = len(partitions_dict)
        try:
            for i in range(partitions_len):
                offset_key = config_env["group_id"]+"_ACD_eventlogtest1"+"_"+str(i)
                count = redis_pool.get(offset_key)
                if not count:
                    redis_pool.set(offset_key, partitions_dict[i][0])
                else:
                    partitions_dict[i]=(int(count),partitions_dict[i][1])
        except Exception as e:
            logger.error(str(e))
        #print(partitions_dict)
        # 分配分区
        one_number = partitions_len / sum_number
        one_number = int(one_number)        
        if sum_number <= partitions_len and id+1 != sum_number:
            sub_partitions_dict = {key:partitions_dict[key] for key in range(id*one_number,id*one_number+one_number)}
        elif sum_number <= partitions_len and id+1 == sum_number:
            sub_partitions_dict = {key:partitions_dict[key] for key in range(id*one_number,partitions_len)}
        else:
            pass
        kfk.kafka_cum_list_init(partitions_dict,"ACD_eventlogtest1")   


    while True:
        #if redis_pool.get("sum_nb") == None:
        #    sum_nb = 0
        #else:
        #    sum_nb = int(redis_pool.get("sum_nb"))
        for i in range(partitions_len):
            datas = kfk.com_data_byoffset(1000,i)
            read_nb = len(datas)
            for data in datas:
                #sum_nb +=1
                if data.error():
                    read_nb -=1
                    
            ##处理数据  如果没有error和none  直接存最后一位redis   检查一下有没有重新读数据   有则seek从redis中读到的数据    
            #kfk.pro_data("ACD_eventlogtest2",[])

            offset_key = config_env["group_id"]+"_ACD_eventlogtest1"+"_"+str(i)
            count = int(redis_pool.get(offset_key))
            if datas != []:
                if data.offset()>=int(count):
                    redis_pool.set(offset_key,count+read_nb )
                    #redis_pool.set("sum_nb",sum_nb )
                else:
                    logger.error("消费异常")
                    redis_pool.set(offset_key, data.offset())

    
        #flag = 0
        #datas = kfk.com_data("inputhbase_Data",10)
        #if datas != [] and datas != None:
        #    while flag == 0:
        #        try:
        #            for data in datas:
        #                if not data.error():
        #                    hbase_data = json.loads(data.value())
        #                    hb.send_bluk({row:value.encode("utf-8") for row , value in hbase_data.items()},"a:0")
        #                else:
        #                    pass
        #            flag = 1
        #        except Exception as e:
        #            logger.error("kafka读数据入hbase失败","        :          ",str(e))
        #            time.sleep(60)
        #else:
        #    pass


    
def main(id,sum_number):
    pg_conf=json.loads(config_env["pg_conn"])
    conn = psycopg2.connect(database=pg_conf["database"], user=pg_conf["user"], password=pg_conf["password"], host=pg_conf["host"], port=pg_conf["port"])
    iep_topic = "ACD_eventlog" 
    hb = Hbaseapi(config_env["hbase_ip"], config_env["hbase_port"])
    kfk = Kafkaapi_redis(config_env["kafka_conn"], config_env["group_id"], str(config_env["timeout"]), config_env["max_bytes"])
    flag,partitions_dict = kfk.partitions_offsetrd(iep_topic)
    while flag == False:
        flag,partitions_dict = kfk.partitions_offsetrd(iep_topic)
        logger.error("查询话题失败   :"+str(partitions_dict))
        time.sleep(5)
    partitions_len = len(partitions_dict)
    try:
        for i in range(partitions_len):
            offset_key = config_env["group_id"]+"_"+iep_topic+"_"+str(i)
            count = redis_pool.get(offset_key)
            if not count:
                redis_pool.set(offset_key, partitions_dict[i][0])
            else:
                partitions_dict[i]=(int(count),partitions_dict[i][1])
    except Exception as e:
        logger.error(str(e))
    # 分配分区
    if id>=sum_number:
        return "error 启动进程数大于分区数" 
    one_number = partitions_len / sum_number
    one_number = int(one_number)        
    if sum_number <= partitions_len and id+1 != sum_number:
        sub_partitions_dict = {key:partitions_dict[key] for key in range(id*one_number,id*one_number+one_number)}
    elif sum_number <= partitions_len and id+1 == sum_number:
        sub_partitions_dict = {key:partitions_dict[key] for key in range(id*one_number,partitions_len)}
    else:
        pass
    kfk.kafka_cum_list_init(partitions_dict,iep_topic)   
    print(sub_partitions_dict)
    
    richdata = Datademo()
    hb.switchTable("iep_event")
    sleep_flag=0
    add_flag = 0
    while True:
        #try:
            #datas=kfk.com_data("ACD_eventlogtest1",1000)
            #TODO  手动offset
        for partition_key in [key for key in sub_partitions_dict.keys()]:
            white=[]
            black=[]
            uuidlist=set()
            hbase_data={}
            white_num=0
            black_num=0
            datas = kfk.com_data_byoffset(1000,partition_key)
            
            read_nb = len(datas)
            add_flag +=read_nb
            logger.info("开始读取话题数据："+iep_topic+"话题"+str(partition_key)+"分区")
        #try:
            if datas == [] or datas == None:
                sleep_flag=1
                hb.close_con()
                logger.info(str(partition_key)+"分区暂无数据  sleep 1秒")

                time.sleep(1)
            else:
                wait_dealdatas={}
                offset_key = config_env["group_id"]+"_"+iep_topic+"_"+str(partition_key)
                origin_offset = int(redis_pool.get(offset_key))
                try:
                    for i in range(len(datas)):
                        if datas[i].error():
                            read_nb -=1
                            print("          -1            ")
                            continue
                        else:
                            vv="%012d"%(datas[i].offset(),)
                            rowkey=hb.random_str(vv)
                            xx=json.loads(datas[i].value())
                            wait_dealdatas[rowkey]=xx
                            hbase_data[rowkey]=datas[i].value()
                            uuidlist.add(xx["client"]["uuid"])
                            data_offset = datas[i].offset()
                    if hbase_data == {}:
                        sleep_flag=1
                        hb.close_con()
                        continue
                    try:
                        if sleep_flag==1:
                            hb.open_con()
                            kfk.reset_pro()
                            sleep_flag=0
                        hb.send_bluk(hbase_data,"a:0")
                        logger.info("数据写入hbase")
                        # ----- 20190929 add    storage  redis --------.
                        try:
                            today = datetime.datetime.now().strftime("%Y-%m-%d")
                            origin_key = 'iep_origin_%s' % today
                            iep_origin_sum = "iep_origin_sum"
                            count = redis_pool.get(origin_key)
                            iep_origin_count = redis_pool.get(iep_origin_sum)
                            if not count:
                                redis_pool.set(origin_key, len(hbase_data))
                            else:
                                redis_pool.set(origin_key, len(hbase_data)+int(count))
                            if not iep_origin_count:
                                redis_pool.set(iep_origin_sum, len(hbase_data))
                            else:
                                redis_pool.set(iep_origin_sum, len(hbase_data)+int(iep_origin_count))
                        except:
                            pass
                    except Exception as e:
                        logger.error(str(e))
                        logger.error(datas[i].topic()+"  "+"入hbase失败")
                        with open(config_env["server_dir"]+"data/hbase_"+str(time.time()), 'w+') as f:
                            json.dump({row:values.decode("utf-8") for row, values in hbase_data.items()}, f)
                        logger.info("hbase数据备份本地成功")  
                except Exception as e:
                    logger.error(str(e))
                uuidlist=list(uuidlist)
                try:
                    cur = conn.cursor()
                    uuid_asset=get_asset(uuidlist,cur)
                except Exception as e:
                    conn.close()
                    conn = psycopg2.connect(database=pg_conf["database"], user=pg_conf["user"], password=pg_conf["password"], host=pg_conf["host"], port=pg_conf["port"])
                    cur = conn.cursor()
                    uuid_asset=get_asset(uuidlist,cur)
                #uuid_asset=get_asset(uuidlist,cur)
                conn.commit()
                cur.close()
                sql_data={}
                uuidlist=[]
                for i in range(len(uuid_asset)):
                    if uuid_asset[i][10] in sql_data:
                        sql_data[uuid_asset[i][10]].append(uuid_asset[i])
                    else:
                        sql_data[uuid_asset[i][10]]=[uuid_asset[i]]
                        uuidlist.append(uuid_asset[i][10])
                for i,j in wait_dealdatas.items():
                    try:
                        richdata = Datademo()
                        if j["type"] == "virus_defend" and len(j["fileinfo"]) >1:
                            for k in j["fileinfo"]:
                                richdata = Datademo()
                                virusfiledata = j.copy()
                                virusfiledata["fileinfo"]=[]
                                virusfiledata["fileinfo"].append(k)
                                flag,data=richdata.tranOldData(virusfiledata,sql_data,uuidlist)
                                data["hbase_rowkey"]=i
                                black.append(json.dumps(data))
                        elif j["type"] != "eventlog":
                            flag,data=richdata.tranOldData(j,sql_data,uuidlist)
                            data["hbase_rowkey"]=i
                            black.append(json.dumps(data))
                        else:
                            flag,data=richdata.tranData(j,sql_data,uuidlist)
                            data["hbase_rowkey"]=i
                            if flag==0:
                                white.append(json.dumps(data))
                            else:
                                black.append(json.dumps(data))
                    except Exception as e:
                        logger.error(str(e))
                        logger.error("数据格式解析错误")
                        with open(config_env["server_dir"]+"data/error_eventlog", 'a+') as f:
                            json.dump(j, f)
                        logger.info("错误数据写入文件")
                if data_offset >= origin_offset:
                    redis_pool.set(offset_key,origin_offset+read_nb )
                    logger.info(iep_topic+"话题"+str(partition_key)+"分区"+str(data_offset))
                else:
                    logger.error("消费异常")
                    logger.error(iep_topic+"话题"+str(partition_key)+"分区"+str(data_offset))
                    redis_pool.set(offset_key,data_offset)
                if white!= []:
                    white_num=kfk.pro_data("Standardization_WhiteData",white)
                if black!= []:
                    black_num=kfk.pro_data("Standardization_BlackData",black)
                    print(kfk.pro_flush(1))
                    today = datetime.datetime.now().strftime("%Y-%m-%d")
                    origin_key = 'iep_black_%s' % today
                    count = redis_pool.get(origin_key)
                    if not count:
                        redis_pool.set(origin_key, len(black))
                    else:
                        redis_pool.set(origin_key, len(black)+int(count))
                if white_num!='erro' and black_num!='erro':
                    logger.info("send data %d" % (len(white)+len(black),))
                    logger.info("send whitedata %d" % (len(white),))
                    logger.info("send blackdata %d" % (len(black),))
                else:
                    logger.info("数据处理出错")
                #break
        #except Exception as e:
        #    logger.error(str(e))
        #    time.sleep(5)
        #    continue
    return id

if __name__=="__main__":
    num = int(config_env["pro_num"])
    #main(0,1)
    #input_kafka_output_hbase(2)
    #kfk = Kafkaapi(config_env["kafka_conn"], config_env["group_id"], str(config_env["timeout"]), config_env["max_bytes"])
    #p = Pool(num)
    #for i in range(num):
    #    p.apply_async(main, args=(i,num))
    #    print(i)
    ##p.apply_async(input_kafka_output_hbase, args=(i,))
    #p.close()
    #p.join()

    
    richdata = Datademo()
    virusfiledata={
  "type": "eventlog",
  "client": {
    "name": "vm-Cent6.6",
    "server_uid": "1973429c-e0cb-4b81-bd21-b0fa52531dd2",
    "ip": "192.168.138.140",
    "company_id": 1,
    "server_ip": "10.255.175.142",
    "group_id": 2,
    "uuid": "96D7ED9A31E94A3988EE02A82E2131BD"
  },
  "data": {
    "a": "1574757953",
    "c": {
      "1": "",
      "2": "",
      "3": "",
      "4": ""
    },
    "b": 1574757953,
    "e": [
      {
        "1": "",
        "2": {
          "1": "F9F0B97314BA128B2128D41275B0EA24"
        }
      }
    ],
    "d": {
      "1": [
        {
          "1": "192.168.138.140",
          "2": "00-0C-29-5E-AD-69"
        }
      ]
    },
    "g": [
      {
        "1": "",
        "2": {
          "1": "F9F0B97314BA128B2128D41275B0EA24"
        }
      }
    ],
    "f": {
      "1": {
        "1": "",
        "2": "",
        "3": "",
        "4": ""
      },
      "3": "",
      "4": "未处理",
      "5": "",
      "6": "",
      "7": ""
    },
    "i": {
      "1": [
        {
          "1": "",
          "2": {
            "1": "F9F0B97314BA128B2128D41275B0EA24",
            "2": "",
            "3": ""
          },
          "3": "/root/Desktop",
          "4": "9999.exe",
          "5": "",
          "6": 73802,
          "7": "",
          "8": "trojan/win32.rozena.ed(ascommon)",
          "9": "Windows",
          "a": "",
          "c": 1574757953,
          "b": "",
          "e": "",
          "d": "",
          "g": {
            "1": "",
            "2": "",
            "3": "",
            "4": {
              "1": "",
              "2": "",
              "3": ""
            }
          },
          "h": {
            "1": "",
            "2": "",
            "3": "",
            "4": "",
            "5": "",
            "6": ""
          },
          "k": ""
        }
      ],
      "2": [],
      "3": [
        {
          "1": 1574757953,
          "2": "96D7ED9A31E94A3988EE02A82E2131BD",
          "3": "IEP",
          "4": {
            "1": "",
            "2": ""
          },
          "5": {
            "1": "未命名公司"
          },
          "6": "源载荷"
        }
      ]
    }
  },
  "ts": 1574757953
}
    virusfiledata11={
  "type": "eventlog",
  "client": {
    "name": "vm-Cent6.6",
    "server_uid": "1973429c-e0cb-4b81-bd21-b0fa52531dd2",
    "ip": "192.168.138.140",
    "company_id": 1,
    "server_ip": "10.255.175.142",
    "group_id": 2,
    "uuid": "96D7ED9A31E94A3988EE02A82E2131BD"
  },
  "data": {
    "a": "1574757953",
    "c": {
      "1": "",
      "2": "",
      "3": "",
      "4": ""
    },
    "b": 1574757953,
    "e": [
      {
        "1": "",
        "2": {
          "1": "F9F0B97314BA128B2128D41275B0EA24"
        }
      }
    ],
    "d": {
      "1": [
        {
          "1": "192.168.138.140",
          "2": "00-0C-29-5E-AD-69"
        }
      ]
    },
    "g": [
      {
        "1": "",
        "2": {
          "1": "F9F0B97314BA128B2128D41275B0EA24"
        }
      }
    ],
    "f": {
      "1": {
        "1": "",
        "2": "",
        "3": "",
        "4": ""
      },
      "3": "",
      "4": "未处理",
      "5": "",
      "6": "",
      "7": ""
    },
    "i": {
      "1": [
        {
          "1": "",
          "2": {
            "1": "F9F0B97314BA128B2128D41275B0EA24",
            "2": "",
            "3": ""
          },
          "3": "/root/Desktop",
          "4": "9999.exe",
          "5": "",
          "6": 73802,
          "7": "",
          "8": "trojan/win32.rozena.ed(ascommon)",
          "9": "Windows",
          "a": "",
          "c": 1574757953,
          "b": "",
          "e": "",
          "d": "",
          "g": {
            "1": "",
            "2": "",
            "3": "",
            "4": {
              "1": "",
              "2": "",
              "3": ""
            }
          },
          "h": {
            "1": "",
            "2": "",
            "3": "",
            "4": "",
            "5": "",
            "6": ""
          },
          "k": ""
        }
      ],
      "2": [],
      "3": [
        {
          "1": 1574757953,
          "2": "96D7ED9A31E94A3988EE02A82E2131BD",
          "3": "IEP",
          "4": {
            "1": "",
            "2": ""
          },
          "5": {
            "1": "未命名公司"
          },
          "6": "源载荷"
        }
      ]
    }
  },
  "ts": 1574757953
}
    virusfiledata["data"]["i"]["1"][0]["2"]["2"] = "7677777777777777777777777777777"
    tmp = richdata.tranData(virusfiledata,{},[])
    #virusfiledata["data"]["i"]["1"][0]["2"]["2"] = "7677777777777777777777777777777"
    #print(json.dumps(tmp,ensure_ascii=False,sort_keys=True, indent=4, separators=(', ', ': ')))
    richdata = Datademo()
    tmp = richdata.tranData(virusfiledata11,{},[])
    #tmp["data"]["i"]["1"][0]["2"]["2"] = "7677777777777777777777777777777"
    print(json.dumps(tmp,ensure_ascii=False,sort_keys=True, indent=4, separators=(', ', ': ')))
    #tmp["data"]["i"]["1"][0]["2"]["2"]
    
                
   



