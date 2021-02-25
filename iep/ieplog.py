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
from reg_uuid import Reg_uuid


from datademo import Datademo
from hbase import Hbaseapi

# 从Config.py中加载进程环境变量
import Config; Config._init()
config_env = Config.get_value()

logger = config_env["logger"]
redis_pool = config_env["redis_pool"]

#kafka api 
class Kafkaapi_redis(object):
    def __init__(self,ip_port,groupid,timeout,max_bytes):
        self.kafka_consumer_conf={
        'bootstrap.servers':ip_port,
        'max.poll.interval.ms':3000000,
        'enable.auto.offset.store':False,
        'group.id':groupid,
        'session.timeout.ms':timeout,
        'fetch.message.max.bytes':max_bytes,
        'default.topic.config':{'auto.offset.reset':"smallest"}
    }
        self.kafka_pro_conf={
        'bootstrap.servers': config_env["kafka_conn"],
        'message.max.bytes':10485760,
        'retries':3,
        'socket.send.buffer.bytes':10000000,
        'enable.idempotence':True

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
            xx=self.pro_flush(5)
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
            partitions_num = len(data.topics[topicname].partitions)
            if len(self.kafka_cum_list) ==0:
                for i in range(partitions_num):
                    self.kafka_cum_list.append(Consumer(self.kafka_consumer_conf))
            return True,partitions_num
        except KeyError as ex:
            logger.error(str(ex)+"没有这个话题")
            return False,str(ex)
        except Exception as ex:
            logger.error(str(ex))            
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
        datas = self.kafka_cum_list[partition_num].consume(num,2)
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

    def close(self):
        self.kafka_cum.close()
        for i in range(len(self.kafka_cum_list)):
            self.kafka_cum_list[i].close()

def get_k_attck(conn,attck_list):
    attack_label={
        "初始访问":"1",
        "执行":"2",
        "持久化":"3",
        "提权":"4",
        "防御规避":"5",
        "凭证访问":"6",
        "发现":"7",
        "横向运动":"8",
        "收集":"9",
        "命令控制":"10",
        "渗透":"11",
        "影响":"12"
        }
    not_get_attck = []
    try:
        cur = conn.cursor()
        sql = "select attck_id,attck_content from k_attck"
        cur.execute(sql,[])
        attck_data = cur.fetchall()
        #print(data)
        datas=[]
        #conn.commit()
        #print([value[1] for value in data])
        data=set([value[1] for value in attck_data])
        for i in attck_list:
            key,techniques = i.split(".")
            if techniques not in data:
                not_get_attck.append([attack_label[key],techniques])
        #print(not_get_attck)
        if not_get_attck != []:
            sql = "insert into k_attck(attck_id,parent_id,attck_content) values((select max(attck_id) from k_attck)+1,%s,%s) returning attck_id,attck_content"
            cur.executemany(sql,not_get_attck)
            conn.commit()
        sql = "select attck_id,attck_content from k_attck order by attck_id"
        cur.execute(sql,[])
        datas = cur.fetchall()
            #print(datas)
        conn.commit()
        cur.close()
        datas = {value[1]:str(value[0]) for value in datas}
    except Exception as e:
        print(str(e))
    return datas

def get_and_res_asset(conn,clientuuidlist,clientiplist,pg_conf,serveruuidlist,asset_api):
    flag = False
    asset = Reg_uuid()
    #print(clientiplist)
    while not flag:
        try:
            cur = conn.cursor()
            #uuid_asset=get_asset(clientuuidlist,cur)
            uuid_asset=get_asset(list(clientiplist.values()),cur)
        except Exception as e:
            conn.close()
            conn = psycopg2.connect(database=pg_conf["database"], user=pg_conf["user"], password=pg_conf["password"], host=pg_conf["host"], port=pg_conf["port"])
            cur = conn.cursor()
            #uuid_asset=get_asset(clientuuidlist,cur)
            uuid_asset=get_asset(list(clientiplist.values()),cur)
        conn.commit()
        cur.close()
        sql_data={}
        uuidlist=[]
        for i in range(len(uuid_asset)):
            if uuid_asset[i][11] in sql_data:
                sql_data[uuid_asset[i][11]].append(uuid_asset[i])
            else:
                sql_data[uuid_asset[i][11]]=[uuid_asset[i]]
                uuidlist.append(uuid_asset[i][11])
        #进行资产注册
        flag = True
        for i in clientuuidlist:
            if i not in sql_data:
                asset_result = asset.getuuid(asset_api,i,clientiplist[i],serveruuidlist[i])
                if asset_result["message"]== "ok":
                    #print(asset_result["message"])
                    sql_data[i] = [("","",clientiplist[i],"","3","","","","","",asset_result["asset_id"],i)]
                else:
                    logger.info("注册资产失败"+"        :"+i)
                    flag = False
    return sql_data,uuidlist


        #获取资产信息

        
def get_asset(data,cur):
    #print(data)
    sql = """select a.os_ver,string_agg(e.software_name,','),f.ip_addr,f.mac_addr,a.asset_level,a.create_person,a.use_person,b.node_name,c.type_name,d.group_name,a.asset_id ,ha.map_asset_id
        from h_asset_source_record as ha
        left join h_hardware_asset_info as a on ha.first_asset_id=a.asset_id
        left join sys_node_info as b on a.node_id=b.node_id 
        left join k_asset_type as c on a.type_id=c.type_id 
        left join (select asset_id,software_id from h_asset_software where install_state='1') as hs on hs.asset_id=a.asset_id
        left join h_software_info as e on e.software_id=hs.software_id
        left join h_asset_ip_info as f on f.asset_id=a.asset_id 
        left join h_asset_group as d on f.group_id=d.group_id 
        where ha.map_asset_id in (select unnest(%s))
        group by a.asset_id,b.node_name,c.type_name,d.group_name,f.ip_addr,f.mac_addr,ha.map_asset_id;"""
    sql = """select a.os_ver,string_agg(e.software_name,','),ipinfo.ip_addr,ipinfo.mac_addr,a.asset_level,a.create_person,a.use_person,b.node_name,c.type_name,d.group_name,a.asset_id ,ha.map_asset_id
        from h_asset_ip_info as ipinfo
        left join h_hardware_asset_info as a on ipinfo.asset_id=a.asset_id
        left join h_asset_source_record as ha on ha.first_asset_id=a.asset_id
        left join sys_node_info as b on a.node_id=b.node_id 
        left join k_asset_type as c on a.type_id=c.type_id 
        left join (select asset_id,software_id from h_asset_software where install_state='1') as hs on hs.asset_id=a.asset_id
        left join h_software_info as e on e.software_id=hs.software_id
        left join h_asset_group as d on ipinfo.group_id=d.group_id 
        where ipinfo.ip_addr in (select unnest(%s)) and a.delete_state = '1'
        group by a.asset_id,b.node_name,c.type_name,d.group_name,ipinfo.ip_addr,ipinfo.mac_addr,ha.map_asset_id;"""
    cur.execute(sql,(data,))
    rows = cur.fetchall()
    return rows

    #测试kafka和资产的接口
def input_kafka_output_hbase(id,sum_number):
    hb = Hbaseapi(config_env["hbase_ip"], config_env["hbase_port"])
    kfk = Kafkaapi_redis(config_env["kafka_conn"], config_env["group_id"], str(config_env["timeout"]), config_env["max_bytes"])
    flag,partitions_dict = kfk.partitions_offsetrd("ACD_eventlog")
    y =0
    topic1 = "Standardization_WhiteData"
    topic2 = "Standardization_BlackData"
    topic3 = "ACD_eventlog"
    for i in range(len(partitions_dict)):
        y+=kfk.kafka_cum.get_watermark_offsets(TopicPartition(topic=topic1,partition=i,offset=0))[1]-kfk.kafka_cum.get_watermark_offsets(TopicPartition(topic=topic1,partition=i,offset=0))[0]
    print("消息总数",y)
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
        kfk.kafka_cum_list_init(partitions_dict,"ACD_eventlog")   


    while True:
        if redis_pool.get("sum_nb") == None:
            sum_nb = 0
        else:
            sum_nb = int(redis_pool.get("sum_nb"))
        for i in range(partitions_len):
            datas = kfk.com_data_byoffset(1000,i)
            read_nb = len(datas)
            for data in datas:
                #sum_nb +=1
                if data.error():
                    read_nb -=1
                    
            ##处理数据  如果没有error和none  直接存最后一位redis   检查一下有没有重新读数据   有则seek从redis中读到的数据    
            kfk.pro_data("ACD_eventlogtest2",[data.value().decode('utf-8')])
            exit(0)
            #offset_key = config_env["group_id"]+"_ACD_eventlogtest1"+"_"+str(i)
            #count = int(redis_pool.get(offset_key))
            #if datas != []:
            #    if data.offset()>=int(count):
            #        redis_pool.set(offset_key,count+read_nb )
            #        #redis_pool.set("sum_nb",sum_nb )
            #    else:
            #        logger.error("消费异常")
            #        redis_pool.set(offset_key, data.offset())
        
def get_event_heartinfo(iep_heart_datas,data):
    timeArray = time.localtime(time.time())
    send_data = str(time.strftime("%Y-%m-%d %H:%M:%S", timeArray))
    if data["client"]["server_uid"] in iep_heart_datas.keys():
        iep_heart_datas[data["client"]["server_uid"]]["device_ip"]=data["client"]["server_ip"]
        iep_heart_datas[data["client"]["server_uid"]]["last_send_time"]=send_data
    else:
        iep_heart_datas[data["client"]["server_uid"]] = {
            "device_ip":data["client"]["server_ip"],
            "first_send_time":send_data,
            "last_send_time":send_data
        }
    return iep_heart_datas

def redis_delete(key):
    redis_pool.delete(key)

def get_event_heartinfos(iep_heart_datas,datas):
    for i in datas.values():
        timeArray = time.localtime(time.time())
        send_data = str(time.strftime("%Y-%m-%d %H:%M:%S", timeArray))
        if i["client"]["server_uid"] in iep_heart_datas.keys():
            iep_heart_datas[i["client"]["server_uid"]]["device_ip"]=i["client"]["server_ip"]
            iep_heart_datas[i["client"]["server_uid"]]["last_send_time"]=send_data
        else:
            iep_heart_datas[i["client"]["server_uid"]] = {
                "device_ip":i["client"]["server_ip"],
                "first_send_time":send_data,
                "last_send_time":send_data
            }
    return iep_heart_datas
    

def heart(data):
    #print(len(data))
    try:
        iep_heart_datas = redis_pool.get("iep")
        #print(type(iep_heart_datas),iep_heart_datas)
        if not iep_heart_datas:
            #heart_data = get_event_heartinfo({},data)
            heart_data = get_event_heartinfos({},data)
            send_data = json.dumps(heart_data)
        else:
            #heart_data = get_event_heartinfo(json.loads(iep_heart_datas),data)
            heart_data = get_event_heartinfos(json.loads(iep_heart_datas),data)
            send_data = json.dumps(heart_data)
        redis_pool.set("iep",send_data)
    except Exception as ex:
        logger.error(str(ex))
        logger.error("记录心跳失败")
    
def main(id,sum_number):          
    pg_conf=json.loads(config_env["pg_conn"])
    valid_ip = config_env["server_ip"]
    conn = psycopg2.connect(database=pg_conf["database"], user=pg_conf["user"], password=pg_conf["password"], host=pg_conf["host"], port=pg_conf["port"])
    iep_topic = "ACD_eventlog" 
    assert_level = 0
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
    richdata = Datademo()
    hb.switchTable("iep_event")
    sleep_flag=0
    add_flag = 0
    while True:
        try:
            #TODO  手动offset
            for partition_key in [key for key in sub_partitions_dict.keys()]:
                white=[]
                black=[]
                clientuuidlist=set()
                serveruuidlist = {}
                attck_list = set()
                clientiplist = {}
                hbase_data={}
                white_num=0
                black_num=0
                datas = kfk.com_data_byoffset(1000,partition_key)
                read_nb = len(datas)
                add_flag +=read_nb
                logger.info("开始读取话题数据："+iep_topic+"话题"+str(partition_key)+"分区")
            #try:
                if datas == [] or datas == None:
                    assert_level +=1 
                    if assert_level >= 600:
                        kfk.close()
                        return -1
                    if datas == None:
                        logger.info("data ================================ None")
                    sleep_flag=1
                    hb.close_con()
                    logger.info(str(partition_key)+"分区暂无数据  sleep 1秒")

                    time.sleep(1)
                else:
                    assert_level = 0
                    wait_dealdatas={}
                    offset_key = config_env["group_id"]+"_"+iep_topic+"_"+str(partition_key)
                    origin_offset = int(redis_pool.get(offset_key))
                    try:
                        for i in range(len(datas)):
                            if datas[i].error():
                                read_nb -=1
                                continue
                            else:
                                data_offset = datas[i].offset()
                                #print(datas[i].offset())
                                if data_offset < origin_offset:
                                    break
                                vv="%012d"%(datas[i].offset(),)
                                rowkey=hb.random_str(vv)
                                xx=json.loads(datas[i].value())
                                #if xx["client"]["server_ip"] not in ["10.255.52.122","10.255.49.17"]:
                                if xx["client"]["server_ip"] not in valid_ip and not valid_ip == "-1":
                                    logger.info("测试数据")
                                    print("测试数据",xx["client"]["server_ip"])
                                    #print(datas[i].value())
                                    logger.info(datas[i].value())
                                    break
                                wait_dealdatas[rowkey]=xx
                                hbase_data[rowkey]=datas[i].value()
                                #xx_attack=xx["data"]["f"]["7"].split(".") if "7" in xx["data"]["f"] and xx["data"]["f"]["7"]!= "" else ['','']
                                clientuuidlist.add(xx["client"]["uuid"])
                                clientiplist[xx["client"]["uuid"]] = str(xx["client"]["ip"])
                                serveruuidlist[xx["client"]["uuid"]] = xx["client"]["server_uid"]
                                if xx["data"]["f"]["7"] !="":
                                    print(xx["data"]["f"]["7"]) 
                                    attck_list.add(xx["data"]["f"]["7"])
                        if hbase_data == {}:
                            sleep_flag=1
                            hb.close_con()
                            continue
                        heart(wait_dealdatas)                        
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
                                logger.error("原始日志计数失败")
                                pass
                        except Exception as e:
                            logger.error(str(e))
                            logger.error(datas[i].topic()+"  "+"入hbase失败")
                            with open(config_env["server_dir"]+"data/hbase_"+str(time.time()), 'w+') as f:
                                json.dump({row:values.decode("utf-8") for row, values in hbase_data.items()}, f)
                            logger.info("hbase数据备份本地成功")  
                    except Exception as e:
                        logger.error(str(e))
                    clientuuidlist=list(clientuuidlist)
                    sql_data,uuidlist = get_and_res_asset(conn,clientuuidlist,clientiplist,pg_conf,serveruuidlist,config_env["asset_url"])
                    attck_dist = get_k_attck(conn,attck_list)
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
                                #j["data"]["i"]["1"][0]["3"] = j["data"]["i"]["1"][0]["3"]+"%E6%B5%8B%E8%AF%95abc"
                                flag,data=richdata.tranData(j,sql_data,uuidlist,attck_dist)
                                #print(attck_dist)
                                #print(json.dumps(data,ensure_ascii=False,sort_keys=True, indent=4, separators=(', ', ': ')))
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
                    if white!= []:
                        white_num=kfk.pro_data("Standardization_WhiteData",white)
                    if black!= []:
                        black_num=kfk.pro_data("Standardization_BlackData",black)
                        today = datetime.datetime.now().strftime("%Y-%m-%d")
                        origin_key = 'iep_black_%s' % today
                        count = redis_pool.get(origin_key)
                        if not count:
                            redis_pool.set(origin_key, len(black))
                        else:
                            redis_pool.set(origin_key, len(black)+int(count))
                    if data_offset >= origin_offset:
                        redis_pool.set(offset_key,origin_offset+read_nb )
                        logger.info(iep_topic+"话题"+str(partition_key)+"分区"+"消费到"+str(data_offset)+"offset")
                    else:
                        logger.error("消费异常")
                        logger.error(iep_topic+"话题"+str(partition_key)+"分区"+str(data_offset)+"   redis存储offset"+str(origin_offset))
                        #redis_pool.set(offset_key,data_offset)
                        #break
                    if white_num!='erro' and black_num!='erro':
                        logger.info("send data %d" % (len(white)+len(black),))
                        logger.info("send whitedata %d" % (len(white),))
                        logger.info("send blackdata %d" % (len(black),))
                    else:
                        logger.error("发送有错误数据")
                #break
        except Exception as e:
            logger.error(str(e))
            time.sleep(5)
            continue
    return id
def control_main(id ,num):
    pass
    while True:
        assert_level = main(id,num)
        if assert_level != -1:
            return
if __name__=="__main__":
    num = int(config_env["pro_num"])
    #redis_delete("iep")
    #main(0,1)
    #input_kafka_output_hbase(2,7)
    #kfk = Kafkaapi_redis(config_env["kafka_conn"], config_env["group_id"], str(config_env["timeout"]), config_env["max_bytes"])
    #pg_conf=json.loads(config_env["pg_conn"])
    #conn = psycopg2.connect(database=pg_conf["database"], user=pg_conf["user"], password=pg_conf["password"], host=pg_conf["host"], port=pg_conf["port"])
    #print(get_k_attck(conn,["命令控制.测试20"]))
    #get_k_attck(conn,["命令控制.测试15"])




    
    p = Pool(num)
    for i in range(num):
        p.apply_async(control_main, args=(i,num))
    p.close()
    p.join()

    


