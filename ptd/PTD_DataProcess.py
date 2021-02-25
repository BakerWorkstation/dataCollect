#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# __author__: zhangyue

import json
from kafka_operating import add_to_kafka,read_from_kafka
import redis
import happybase
import time
import hashlib
import os
import datetime
import psycopg2
import psycopg2.extras
import requests
from struct import unpack
from socket import AF_INET,inet_pton,inet_aton
from multiprocessing import Process
from urllib.parse import urlparse
import urllib.parse
import re
import copy


# 从Config.py中加载进程环境变量
import Config; Config._init()
config_env = Config.get_value()

kafka_topic = config_env["topic"]
group_id = config_env["group_id"]
reset = config_env["reset"]
redis_pool = config_env["redis_pool"]

logger = config_env["logger"]

global process_id
process_id = os.getpid()   # 获取主进程进程id

# hbase获取句柄
# hbase_conn = happybase.Connection(host=config_env["hbase_ip"], port=config_env["hbase_port"])

# postgres获取句柄
pg_conf = json.loads(config_env["pg_conn"])
# conn = psycopg2.connect(database=pg_conf["database"], user=pg_conf["user"], password=pg_conf["password"], host=pg_conf["host"], port=pg_conf["port"])
# cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# 生成资产API
asset_api = config_env["asset_reg"]


# 匹配本应该是list的字段是否是null值 如果是null则输出空字符串，如果是正常列表则返回列表中的值
def list_null(obj):
    if isinstance(obj,list):
        if obj !=[]:   # 匹配取到的值是否是[]如果是则变成空字符串
            return obj[0]
        else:
            return ""
    else:
        return ""
# 匹配本应该是string的字段是否是null值 如果是null则输出空字符串，如果是正常字符则返回该值
def str_null(obj):
    #if isinstance(obj,basestring):
    if isinstance(obj,(str, bytes)):
        return obj
    else:
        return ""
# 匹配本应该是[{}]的字段是否是null值 如果是null则输出[{}]，如果是正常[{},{}……]的形式则返回该值
def list_dict_null(obj):
    if isinstance(obj,list):
        return obj
    else:
        return [{}]

# --------------------------封装hbase方法--------------------------
class HbaseManul(object):
    def __init__(self, ip, port):
        self.conn = happybase.Connection(host=ip, port=port,protocol='compact',transport='framed')
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

# hbase写入数据方法，当有数据时打开句柄，没有数据是关闭句柄，避免thrift报错
def write2Hbase(rowkey, message,hbase_tube):
    try:
        hBase = HbaseManul(config_env["hbase_ip"],config_env["hbase_port"])
    except Exception as e:
        logger.error('message: %s -> HBase写入失败 主程序退出' % str(e))
        os.killpg(os.getpgid(process_id), 9)
    try:
        hBase.switchTable(hbase_tube)
        hBase.putData(rowkey, message)
    except Exception as e:
        logger.error('message: %s -> HBase写入失败 主程序退出' % str(e))
        os.killpg(os.getpgid(process_id), 9)
    finally:
        hBase.close()

# --------------------判断攻击者是源端点还是目的端点----------------------
def get_threat_scene(data):
    raw_data = data
    d_details = raw_data.get("d")
    if d_details:
        d_file = d_details.get("file")  # 一个列表
        d_sengine = d_details.get("sengine")  # 一个列表
    else:
        d_file = None
        d_sengine = None
    attack = "dst"  # 都先假定攻击者都是原始日志下的dst字段，根据不同场景在修改
    try:
        raw_data.get("msg").get("download")
        msg_download_flags = True
    except AttributeError as e:
        msg_download_flags = False
    proto_list = raw_data.get("proto")
    if d_file is not None:
        # 虽然确定了是恶意样本，但是并不能给出攻击者和受害者的具体区别譬如说SMTP和IMAP的结果应该就是相反的
        virus_family = d_file[0].get("virus_family")
        if proto_list[0] in ["SMTP", "IMAP", "POP", "POP3"]:
            # 判断是否为发送邮件或者接收邮件，进一步确定攻击方和受害方
            # 为True说明是发送邮件，此时攻击者是src, 受害者是dst
            if "SMTP" in raw_data.get("proto"):
                # 此时的攻击者变了！！，因为发送了恶意邮件
                attack = "src"
        elif "FTP" in raw_data.get("proto"):
            try:
                if raw_data.get("msg",{}).get("upload",{}).get("md5"):
                    # 上传文件，出了内鬼，此时的攻击者为src
                    attack = "src"
            finally:
                pass
        elif msg_download_flags and raw_data.get("msg",{}).get("upload",{}).get("md5","") != "":
            # 又出了内鬼，传了奇怪的文件
            # 上传
            if proto_list[0] == "SMB":
                attack = "src"
            elif proto_list[0] == "NFS":
                attack = "src"
            else:
                # 下载
                attack = "dst"
    # 继续判断是否为利用漏洞
    elif d_sengine is not None and isinstance(d_sengine, list):
        # 利用漏洞都是src是攻击方，dst是受害方
        attack = "src"
    # threat_scene = tmpdict
    return attack

# ------------------------------判断事件类别-------------------------------------
def get_threat_type(raw_data):
    """
    返回四item的字典
    :param one_log:
    :return:
    """
    d_details = raw_data.get("d")  # d字段下的数据
    if d_details:
        d_file = d_details.get("file")  # 一个列表
        d_url = d_details.get("url")  # 一个列表
        d_ip = d_details.get("ip")  # 一个列表
        d_sengine = d_details.get("sengine")  # 一个列表
        d_domain = d_details.get("domain")  # 一个列表
    else:
        return None
    tmpdict = {

    }
    c2_flag = False  # 是否为C2
    md5_flag = False  # 是否为恶意样本
    vul_flag = False  # 是否为利用漏洞
    attack = "dst"  # 都先假定攻击者都是原始日志下的dst字段，根据不同场景在修改
    try:
        raw_data.get("msg").get("download")
        msg_download_flags = True
    except AttributeError as e:
        msg_download_flags = False
    proto_list = raw_data.get("proto")
    # c2的条件是最宽松的,你既然是某一种恶意行为，那必然是三种中的一种，先给出一个保底的，然后在慢慢筛选条件，找出最贴切的
    # if d_url is not None or d_domain is not None or d_ip is not None or check_sengine_cnc(
    #         d_sengine):
    # 把引擎的检出不算入威胁场景
    if d_url is not None or d_domain is not None or d_ip is not None:
        # 这就是C2
        tmpdict = {
            "stage": "C2",
            "objective": u"命令与控制",
            "action": u"利用僵尸网络",
            "key_phrases": u"僵尸网络C2",
        }
        # 此时攻击者依然是dst
        c2_flag = True

    # 在判断是否为恶意样本,
    elif d_file is not None:
        md5_flag = True
        # 虽然确定了是恶意样本，但是并不能给出攻击者和受害者的具体区别譬如说SMTP和IMAP的结果应该就是相反的
        virus_family = d_file[0].get("virus_family")
        if "HTTP" in raw_data.get("proto"):
            # HTTP协议依然维持攻击者是dst不变
            if virus_family == "GrayWare" or virus_family == "RiskWare" or virus_family == "JunkFile" or virus_family == "TestFile":
                tmpdict = {
                    "stage": u"突破",  # src攻击者，dst受害
                    "objective": u"投递",
                    "action": u"网站植入",
                    "key_phrases": u"广告软件",
                }
            else:
                tmpdict = {
                    "stage": u"突破",
                    "objective": u"投递",
                    "action": u"网站植入",
                    "key_phrases": u"下载",
                }

        elif proto_list[0] in ["SMTP", "IMAP", "POP", "POP3"]:
            # 判断是否为发送邮件或者接收邮件，进一步确定攻击方和受害方
            # 为True说明是发送邮件，此时攻击者是src, 受害者是dst
            if "SMTP" in raw_data.get("proto"):
                tmpdict = {
                    "stage": u"突破",
                    "objective": u"利用",
                    "action": u"利用受感染主机",
                    "key_phrases": u"木马",
                }
                # 此时的攻击者变了！！，因为发送了恶意邮件
                attack = "src"
            else:
                # 差不多是IMAP协议，接受邮件的，维持原判，攻击者是dst不变
                tmpdict = {
                    "stage": u"突破",
                    "objective": u"投递",
                    "action": u"发送恶意邮件",
                    "key_phrases": u"恶意附件",
                }
        elif "FTP" in raw_data.get("proto"):
            try:
                if raw_data.get("msg").get("upload").get("md5"):
                    # 上传文件，出了内鬼，此时的攻击者为src
                    tmpdict = {
                        "stage": u"存在",
                        "objective": u"横向移动",
                        "action": u"写入远程文件共享",
                        "key_phrases": u"FTP",
                    }
                    attack = "src"
                else:
                    # 从ftp下载文件，攻击者依旧是dst
                    tmpdict = {
                        "stage": u"突破",
                        "objective": u"投递",
                        "action": u"利用合法远程访问",
                        "key_phrases": u"FTP",
                    }
            except AttributeError as e:
                # 维持原判，攻击者还是dst
                pass

        elif msg_download_flags and raw_data.get("msg").get("upload").get("md5") != "":
            # 又出了内鬼，传了奇怪的文件
            # 上传

            if proto_list[0] == "SMB":
                attack = "src"
                if raw_data.get("malname") and "Virus" in raw_data.get("malname")[0]:
                    tmpdict = {
                        "stage": u"存在",
                        "objective": u"横向移动",
                        "action": u"污染共享内容",
                        "key_phrases": u"嵌入式脚本",
                    }
                elif raw_data.get("malname") and "Exploit" in raw_data.get("malname")[0]:
                    tmpdict = {
                        "stage": u"存在",
                        "objective": u"横向移动",
                        "action": u"污染共享内容",
                        "key_phrases": u"向文件添加漏洞利用",
                    }
                else:
                    tmpdict = {
                        "stage": u"存在",
                        "objective": u"横向移动",
                        "action": u"写入远程文件共享",
                        "key_phrases": u"CIFS",
                    }
            elif proto_list[0] == "NFS":
                attack = "src"
                if raw_data.get("malname") and "Virus" in raw_data.get("malname")[0]:
                    tmpdict = {
                        "stage": u"存在",
                        "objective": u"横向移动",
                        "action": u"污染共享内容",
                        "key_phrases": u"嵌入式脚本",
                    }
                elif raw_data.get("malname") and "Exploit" in raw_data.get("malname")[0]:
                    tmpdict = {
                        "stage": u"存在",
                        "objective": u"横向移动",
                        "action": u"污染共享内容",
                        "key_phrases": u"向文件添加漏洞利用",
                    }
                else:
                    tmpdict = {
                        "stage": u"存在",
                        "objective": u"横向移动",
                        "action": u"写入远程文件共享",
                        "key_phrases": u"NFS",
                    }
            elif proto_list[0] == "FEIQ":
                tmpdict = {
                    "stage": u"突破",
                    "objective": u"投递",
                    "action": u"使用聊天服务",
                    "key_phrases": u"利用服务进行鱼叉式钓鱼攻击",
                }
            else:
                # 下载
                attack = "dst"
                if raw_data.get("proto")[0] == "SMB":
                    tmpdict = {
                        "stage": u"存在",
                        "objective": u"横向移动",
                        "action": u"写入远程文件共享",
                        "key_phrases": u"CIFS",
                    }
                elif raw_data.get("proto")[0] == "NFS":
                    tmpdict = {
                        "stage": u"存在",
                        "objective": u"横向移动",
                        "action": u"写入远程文件共享",
                        "key_phrases": u"NFS",
                    }
                elif raw_data.get("proto")[0] == "FEIQ":
                    tmpdict = {
                        "stage": u"突破",
                        "objective": u"投递",
                        "action": u"使用聊天服务",
                        "key_phrases": u"利用服务进行鱼叉式钓鱼攻击",
                    }

    # 继续判断是否为利用漏洞
    elif d_sengine is not None and isinstance(d_sengine, list):
        # 利用漏洞都是src是攻击方，dst是受害方
        attack = "src"
        vul_flag = True  # 利用漏洞，这里不准确，但为了方便处理这里假设为True，在
        for each_seagine in d_sengine:
            msg = each_seagine.get("msg")
            # 6.10更新，由于换了卡夫卡，读取出来的是编码过后的str,而不是unicode。
            # 因此先尝试解码为unicode，在进行匹配

            if "Cicso" in msg or "Huawei" in msg or u"思科" in msg or u"华为" in msg:
                tmpdict = {
                    "stage": u"突破",
                    "objective": u"利用",
                    "action": u"利用固件漏洞",
                    "key_phrases": u"嵌入式设备",
                }
            elif each_seagine.get("classtype") == "web-application-attack":
                if "SQL" in msg.upper():
                    tmpdict = {
                        "stage": u"突破",
                        "objective": u"投递",
                        "action": u"注入数据库命令",
                        "key_phrases": u"SQL注入",
                    }
                else:
                    tmpdict = {
                        "stage": u"突破",
                        "objective": u"利用",
                        "action": u"利用远程应用程序漏洞",
                        "key_phrases": u"Web应用程序",
                    }
    tmpdict.update({
        "c2_flag": c2_flag,
        "md5_flag": md5_flag,
        "vul_flag": vul_flag,
        "attack": attack,
        "msg_download_flags": msg_download_flags
    })
    if md5_flag or vul_flag:
        if md5_flag:
            return {
                "md5_flag": md5_flag,
                "keyword": tmpdict.get("action")
            }
        else:
            return {
                "vul_flag": vul_flag,
                "keyword": tmpdict.get("action")
            }
    if c2_flag:
        return {
            "c2_flag": c2_flag,
            "keyword": tmpdict.get("action")
        }
    return None
# ------------------------------判断ip是否为内部网络-------------------------------------
def check_private_addr(ip):
        """
        判断ip是否是内网地址，若返回2的话则为内网ip，若返回1则是外部网络ip
        """
        f = unpack('!I', inet_pton(AF_INET, ip))[0]
        '''
        下面网段可选
        '''
        private = (

            [2130706432, 4278190080],  # 127.0.0.0,   255.0.0.0   http://tools.ietf.org/html/rfc3330
            [3232235520, 4294901760],  # 192.168.0.0, 255.255.0.0 http://tools.ietf.org/html/rfc1918
            [2886729728, 4293918720],  # 172.16.0.0,  255.240.0.0 http://tools.ietf.org/html/rfc1918
            [167772160, 4278190080],   # 10.0.0.0,    255.0.0.0   http://tools.ietf.org/html/rfc1918
        )  
        for net in private:
            if (f & net[1]) == net[0]:
                return 2
        return 1

# --------------------------获取关联资产信息--------------------------
def get_asset_data(ip_info,mac_info,dev_info,hb_rowkey,cur,conn):
    try:
        mac_info = re.sub(r"(?<=\w)(?=(?:\w\w)+$)", "-", mac_info.upper())
    except Exception as e:
        logger.error("mac error, message: %s" % str(e)) 
    asset_info = []
    # 只根据ip确定资产
    cur.execute("SELECT DISTINCT asset_id,group_id FROM h_asset_ip_info WHERE ip_addr=%s",(ip_info,))
    uuid_git_list = cur.fetchone()    
    if uuid_git_list:       
        uuid = uuid_git_list[0]
        gid = uuid_git_list[1]
        cur.execute("SELECT ip_addr,mac_addr FROM h_asset_ip_info WHERE asset_id = %s",(uuid,))
        ip_mac_list = cur.fetchall() 
        
        cur.execute("SELECT os_ver,asset_level,create_person,use_person,node_id,type_id FROM h_hardware_asset_info WHERE asset_id=%s",(uuid,))
        asset_data_list = cur.fetchone()
        os_ver = asset_data_list[0]
        asset_level = asset_data_list[1]
        user = asset_data_list[2]
        manager = asset_data_list[3]
        node_id = asset_data_list[4]
        type_id = asset_data_list[5]
        if node_id and type_id:
            cur.execute("SELECT n.node_name,k.type_name FROM sys_node_info as n,k_asset_type as k WHERE n.node_id=%s and k.type_id=%s",(asset_data_list[4],str(asset_data_list[5])))
            un_dev_list = cur.fetchone()
            unit = un_dev_list[0]
            dev_type = un_dev_list[1]
        else:
            cur.execute("SELECT node_name FROM sys_node_info  WHERE node_id=%s",(node_id,))
            un_list = cur.fetchone()
            unit = un_list[0]
            dev_type = ""

        cur.execute("SELECT group_name FROM h_asset_group WHERE group_id=%s",(gid,))
        network_struct = cur.fetchone()

        # cur.execute("SELECT software_name FROM h_software_info WHERE asset_id=%s",(uuid,))
        # software_list = cur.fetchone()
        cur.execute("SELECT software_name FROM h_asset_software as ass left JOIN h_software_info as si on ass.software_id = si.software_id WHERE ass.asset_id=%s and ass.install_state='1'",(uuid,))
        software_list_erwei = cur.fetchall()
        software_list = []
        for software in software_list_erwei:
            software_list.append(software[0])        
            
        asset_info.append(uuid)
        asset_info.append(os_ver)
        asset_info.append(ip_mac_list)
        asset_info.append(unit)
        asset_info.append(network_struct)
        asset_info.append(dev_type)
        asset_info.append(asset_level)
        asset_info.append(user)
        asset_info.append(manager)
        asset_info.append(software_list)

        # conn.close()
        conn.commit()
        cur.close()        
        return asset_info         
    else:
        #先查一下设备id对应的组织机构标识 若查不到则写为“1”
        cur.execute("SELECT node_id FROM h_device_info WHERE device_id=%s",(dev_info,))
        nodeID_list = cur.fetchone()
        if nodeID_list:
            nodeID = nodeID_list[0]
        else:
            nodeID = "1"        
        # 当dev设备id和ip一定时生成的uuid是一样的
        product_uuid = hashlib.md5((dev_info + ip_info).encode("utf-8")).hexdigest().lower()
        url_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 '
                        '(KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
            'Content-Type': 'application/json'
        }
        if External_Asset(ip_info) == "waibuzichan":
            asset_gid = "3"
        else:
            asset_gid = "2"

        propertykeys ={
            "uuid": product_uuid,                          # 资产标识
            "source": "2",                                 # 资产来源 "0": iep "1": 人工录入 "2":ptd "3":cmdb "4":防火墙
            "model": "",                                   # 资产型号
            "level": "3",                                  # 资产等级
            "basepolicy": "",                              # 基准策略  目前可传空
            "type": None,                                  # 资产类型(不能"")
            "gid": asset_gid,                                 # 资产组标识  "1":顶级域 "2":内网域 "3":外网域
            "ip": [ip_info],                               # 资产ip列表
            "host_ip": None,                            # 如果资产分类为虚拟资产则有宿主机ip
            "mac": [mac_info],                             # mac地址列表
            "child_group": ["1"],                       # 资产组子域地址列表
            "special": "",                                 # 资产分类   "1": 虚拟设备  "2": 实体设备
            "oid":nodeID,                                     # 组织机构标识!!!
            "position": "",                                # 部署位置
            "cperson_name": dev_info,                      # 创建人（此处为PTD设备id）
            "cperson_tel": "",                             # 创建人联系方式
            "mperson_name": "",                            # 管理人
            "mperson_tel": "",                             # 管理人联系方式
            "title": []                                    # 资产标签列表
        }
        json_propertykeys = json.dumps(propertykeys)

        try:
            response = requests.post(asset_api, data=json_propertykeys, headers=url_headers,timeout=5)
            # print(response.status_code)
            # print response.json()["message"]
        except:
            logger.warning('hbase_rowkey:%s,内网资产生成失败(调用生成资产API),uuid:%s'%(hb_rowkey,product_uuid))
            # print('hbase_rowkey:%s,此事件中内网资产生成失败(调用生成资产API,插入数据异常)'%(hb_rowkey))
            return False
        else:
            if response.status_code == 200:
                uuid_getfrom_webapi = json.loads(response.text).get("asset_id")
                return [uuid_getfrom_webapi,"",[[propertykeys["ip"][0],propertykeys["mac"][0]]],None,[],"",propertykeys["level"],propertykeys["cperson_name"],"",[]]
            else:
                logger.warning('hbase_rowkey:%s,内网资产生成失败(返回的状态码非200),uuid:%s'%(hb_rowkey,product_uuid))
                # print('%s:此事件中内网资产生成失败(返回的状态码非200)'%(hb_rowkey))
                return False

# 攻击者判断函数 如果攻击者是dst或受害者是src 则将目的端点和源端点互换
attacker_change_flag = False
def attacker_src_dst(dict_i):
    global attacker_change_flag
    if dict_i.get("avlist"):   # 取值判定优先级：avlist > d.sengine.related.attacker > get_threat_scene()
        for avlist_i in dict_i.get("avlist"):
            if avlist_i.get("victim",{}).get("name") == dict_i.get("src").get("ip"):  #因为attacker.name值不一定为ip可能为domain等所以此处匹配受害者是否是src
                kong_dict = {}
                kong_dict = dict_i.get("dst")
                dict_i["dst"] = dict_i.get("src")
                dict_i["src"] = kong_dict
                attacker_change_flag = True
                return dict_i
            else:
                return dict_i
    if dict_i.get("d",{}).get("sengine"):
        for attack_i in dict_i.get("d",{}).get("sengine"):
            attacker_from_ptd = attack_i.get("related").get("attacker")
            if attacker_from_ptd:   # 在d.sengine.related.attacker中有值时
                if attacker_from_ptd[0] == dict_i.get("dst").get("ip"):
                    kong_dict = {}
                    kong_dict = dict_i.get("dst")
                    dict_i["dst"] = dict_i.get("src")
                    dict_i["src"] = kong_dict
                    attacker_change_flag = True
                    return dict_i
                else:
                    return dict_i
            else:   # 在d.sengine.related.attacker中为null时
                if get_threat_scene(dict_i) == "dst": 
                    kong_dict = {}
                    kong_dict = dict_i.get("dst")
                    dict_i["dst"] = dict_i.get("src")
                    dict_i["src"] = kong_dict
                    attacker_change_flag = True
                    return dict_i
                else:
                    return dict_i
    else:
        if get_threat_scene(dict_i) == "dst": 
            kong_dict = {}
            kong_dict = dict_i.get("dst")
            dict_i["dst"] = dict_i.get("src")
            dict_i["src"] = kong_dict
            attacker_change_flag = True
            return dict_i
        else:
            return dict_i

# 判断是否是公司的外部资产
def External_Asset(str_ip):   
    int_ip = unpack('!I', inet_aton(str_ip))[0]
    # 1.189.209.202 - 1.189.209.254 公司外部资产的ip范围
    if 29217226 <= int_ip and int_ip <= 29217278:
        return "waibuzichan"

# --------------------------开始从kafka读取ptd原始日志并处理数据--------------------------
def main():
    # 获取pg句柄
    conn = psycopg2.connect(database=pg_conf["database"], user=pg_conf["user"], password=pg_conf["password"], host=pg_conf["host"], port=pg_conf["port"])
    # cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # 存放处理过后的数据用，当列表中数据积累到一定量之后再调用add_to_kafka函数
    BlackData_List = []
    WhiteData_List = []
    
    # 从kafka中读PTD源数据
    for i in read_from_kafka(kafka_topic,group_id,reset):
        source_data = json.loads(i)
        # print(source_data)

        # ----- 20191016 add    storage  redis 记录每天处理的数据量存进redis---------
        try:
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            origin_key = 'ptd_origin_%s' % today
            count = redis_pool.get(origin_key)
            if not count:
                redis_pool.set(origin_key, 128)
            else:
                redis_pool.set(origin_key, 128+int(count))
            # ----- 2019.11.1 add redis 记录总数据量存进redis---------
            total_key = 'ptd_origin_sum'
            total_count = redis_pool.get(total_key)
            if not total_count:
                redis_pool.set(total_key, 128)
            else:
                redis_pool.set(total_key, 128+int(total_count))            
        except:
            logger.error("Redis Record Failed!!!!")
        
        # ptd设备列表
        # ptd_dev_list = ["BCNHYW2","CJXJWW2","2102311QGK10HA000177","BC3CYW2","BBMHYW2","1NKVRT2","C0GDWW2"]
        ptd_dev_list = config_env["access_dev"].split(",")                     
        for each_dict in source_data["data"]:
            #---------------------过滤不在ptd设备列表中的上报的数据---------------------
            if "-1" not in ptd_dev_list:
                if each_dict.get("dev") not in ptd_dev_list:
                    break             
            #---------------------过滤心跳日志---------------------
            if each_dict.get("type") == "heartbeat":
                continue            
            # --------------------过滤掉白数据，只留黑数据（针对于华能公司需求）-----------------------
            if not (each_dict.get("is_malicious") or each_dict.get("alert")):
                continue

            # ----- 20191129 add ---redis记录设备存活状态进redis---------
            try:
                dev_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                dev_key = 'ptd'
                device_id = each_dict.get("dev")
                dev_redis_data = redis_pool.get(dev_key)
                
                if not dev_redis_data:
                    dev_redis_data_dict = {
                                            device_id:{
                                                "device_ip": "",
                                                "first_send_time": dev_time,
                                                "last_send_time": dev_time
                                            }
                                        }
                    redis_pool.set(dev_key, json.dumps(dev_redis_data_dict))                   
                else:
                    dev_redis_data = json.loads(dev_redis_data)
                    if dev_redis_data.get(device_id): #判断该ptd设备是否已有记录 如果有则更新上报时间 否则在ptd这个redis key下新增一个“device_id”
                        dev_redis_data[device_id]["last_send_time"] = dev_time                    
                        redis_pool.set(dev_key, json.dumps(dev_redis_data))
                    else:
                        dev_redis_data[device_id] = {
                                                    "device_ip": "",
                                                    "first_send_time": dev_time,
                                                    "last_send_time": dev_time
                                                }
                        redis_pool.set(dev_key, json.dumps(dev_redis_data))                                
            except:
                logger.error("Redis About dev Record Failed!!!!")

            # --------------------将原始数据生成rowkey后存入Hbase--------------------------
            m = hashlib.md5()
            # 将ptd中事件“唯一”标识“id”+当前时间戳小数点后的数的倒序hash处理生成32位的md5然后截取中间的16位作为rowkey
            m.update((str(each_dict["id"])+str(time.time())[11:][::-1]).encode("utf-8"))
            hbase_rowkey = m.hexdigest()[8:-8]
            try:
                if each_dict.get("is_malicious") or each_dict.get("alert"):
                    write2Hbase(hbase_rowkey,{"info:content": json.dumps(each_dict)},"PTD_Source_BlackData")
                    logger.info("SourceData_HbaseRowKey is %s"%(hbase_rowkey))
                else:
                    # write2Hbase(hbase_rowkey,{"info:content": json.dumps(each_dict)},"PTD_Source_WhiteData")
                    pass            # 白日志不存hbase       
            except IOError as e:
                logger.error('Hbase data processing failed:%s'%(e))
                os.killpg(os.getpgid(process_id), 9)   # 当写入hbase失败时结束服务进程      

            # --------------------------数据标准化模板--------------------------------------
            with open("./eventlog_template.json",'r') as load_f:
                eventlog_template_dict = json.load(load_f)
            eventlog_template_dict["hbase_rowkey"] = hbase_rowkey
            # --------------------------载荷标签中文映射--------------------------------------
            with open("./map.json",'r') as behavior_map:
                behavior_map_dict = json.load(behavior_map)    
            # --------------------------调用判断攻击者函数重新生成PTD数据----------------------------------
            each_dict = attacker_src_dst(each_dict)

            # --------------------------------es索引要用的数据-----给ES提供的数据--------------------------------
            eventlog_template_dict["Used_by_ES_src"]["source_endpoint_ip"] = each_dict.get("src",{}).get("ip")
            logger.info("src_ip is %s"%(each_dict.get("src").get("ip")))
            # eventlog_template_dict["Used_by_ES_src"]["source_endpoint_domain"] = list_null(each_dict.get("domain"))             
            # eventlog_template_dict["Used_by_ES_src"]["source_endpoint_url"] = urllib.parse.unquote(list_null(each_dict.get("url")))
            # ---------------------当原始数据中url数据但domain中没有数据时，截取url中的domain添加到模板对应位置中---------------------
            # if eventlog_template_dict["Used_by_ES_src"]["source_endpoint_domain"] == "" and not eventlog_template_dict["Used_by_ES_src"]["source_endpoint_url"] == "":
            #     eventlog_template_dict["Used_by_ES_src"]["source_endpoint_domain"] = urlparse(eventlog_template_dict["Used_by_ES_src"]["source_endpoint_url"]).netloc            
            eventlog_template_dict["Used_by_ES_src"]["source_endpoint_port"] = each_dict.get("src",{}).get("port")
            # eventlog_template_dict["Used_by_ES_src"]["source_endpoint_ip_cp"] = each_dict.get("src",{}).get("unit")
            eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_ip"] = each_dict.get("dst").get("ip")
            logger.info("dst_ip is %s"%(each_dict.get("dst").get("ip")))
            # eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_domain"] = list_null(each_dict.get("domain"))
            # eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_url"] = list_null(each_dict.get("url"))
            # # ---------------------当原始数据中url数据但domain中没有数据时，截取url中的domain添加到模板对应位置中---------------------
            # if eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_domain"] == "" and not eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_url"] == "":
            #     eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_domain"] = urlparse(eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_url"]).netloc
            eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_port"] = each_dict.get("dst",{}).get("port")
            # eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_ip_cp"] = each_dict.get("dst",{}).get("unit")
            eventlog_template_dict["Used_by_ES_ExtendInfo"]["is_malicious"] = each_dict.get("is_malicious") 
            eventlog_template_dict["Used_by_ES_ExtendInfo"]["alert"] = each_dict.get("alert")
            eventlog_template_dict["Used_by_ES_ExtendInfo"]["detect_pro_id"] = each_dict.get("dev")
            # eventlog_template_dict["Used_by_ES_ExtendInfo"]["rule_id"] = ""

            label_list_5 = ["跨境通讯","动态域名","邮件通讯","跨域邮件","带链接邮件"]
            if each_dict.get("label") in label_list_5:
                eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_label"].append("user_defined"+":"+each_dict.get("label"))
                eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_name"].append("user_defined"+":"+each_dict.get("label"))
            if len(each_dict.get("file")) >= 1:
            # ================2020.2.21修改====================
                md5_and_filedata = {}   # 构造字典{"md5":file字段信息}用于丰富载荷
                for f in each_dict.get("file"):
                    if f:
                        md5_and_filedata[f.get("md5")] = f
            else:
                md5_and_filedata = None
     
            # --------------------------------处理"d"字段中的数据丰富载荷信息--------------------------------
            # 需要先根据攻击者是本地机器还是外部网络判定d字段下的载荷信息是属于源载荷还是目的载荷
            # 调用函数get_threat_scene()判断攻击者
            eventlog_template_dict["threat_info"]["extend_details"]["attack_info"]["from_ip"]["ip"] = each_dict.get("src",{}).get("ip","")
            eventlog_template_dict["threat_info"]["extend_details"]["attack_info"]["from_ip"]["country"] = each_dict.get("src",{}).get("country","")
            eventlog_template_dict["threat_info"]["extend_details"]["attack_info"]["from_ip"]["city"] = each_dict.get("src",{}).get("city","")
            eventlog_template_dict["threat_info"]["extend_details"]["attack_info"]["from_ip"]["region"] = each_dict.get("src",{}).get("region","")
            eventlog_template_dict["threat_info"]["extend_details"]["attack_info"]["from_ip"]["units"] = each_dict.get("src",{}).get("unit","")
            eventlog_template_dict["threat_info"]["extend_details"]["attack_info"]["from_ip"]["operators"] = each_dict.get("src",{}).get("isp","")
            eventlog_template_dict["threat_info"]["extend_details"]["attack_info"]["from_ip"]["longitude"] = each_dict.get("src",{}).get("location",{}).get("lon","")
            eventlog_template_dict["threat_info"]["extend_details"]["attack_info"]["from_ip"]["latitude"] = each_dict.get("src",{}).get("location",{}).get("lat","")                
            
            # d_dict = eventlog_template_dict["threat_info"]["source_load"][0]  #加载源载荷中的数据模板
            # loader_info_dict = eventlog_template_dict["threat_info"]["extend_details"]["loader_info"][0]  #加载扩展详情中的载荷详情数据模板

            d_have_load_sign = False    # 用来标志在d字段中是否查询到关于载荷的信息
            if each_dict.get("d",{}).get("url"):
                eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_label"].append("C2引擎:C2通信")
                eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_name"].append("C2引擎:C2通信")                
                d_have_load_sign = True                
                for each_d in each_dict.get("d",{}).get("url"):
                    d_dict = copy.deepcopy(eventlog_template_dict["threat_info"]["source_load"][0])  #加载源载荷中的数据模板
                    loader_info_dict = copy.deepcopy(eventlog_template_dict["threat_info"]["extend_details"]["loader_info"][0])  #加载扩展详情中的载荷详情数据模板
                    eventlog_template_dict["Used_by_ES_src"]["source_load_attack_action"] = each_d.get("virus_behaviors")  # 给ES提供的数据
                    eventlog_template_dict["Used_by_ES_src"]["source_load_attack_v_name"] = each_d.get("malname")         # 给ES提供的数据
                    eventlog_template_dict["Used_by_ES_src"]["source_load_attack_v_type"] = each_d.get("virus_type")         # 给ES提供的数据
                    eventlog_template_dict["Used_by_ES_src"]["source_load_attack_v_family"] = each_d.get("virus_family")         # 给ES提供的数据

                    d_dict["v_name"] = each_d.get("malname")
                    d_dict["v_type"] = each_d.get("virus_type")
                    d_dict["v_family"] = each_d.get("virus_family")
                    d_dict["platform"] = each_d.get("virus_platform")
                    d_dict["attack_action"] = each_d.get("virus_behaviors")
                    d_dict["detect"]["detect_time"] = each_dict.get("ts",{}).get("start")
                    d_dict["detect"]["detect_pro"] = "PTD"
                    d_dict["detect"]["detect_pro_id"] = each_dict.get("dev")
                    d_dict["detect"]["detect_rule"]["detect_base"] = each_d.get("author") 
                    d_dict["detect"]["detect_rule"]["detect_id"] = each_d.get("rid")
                    d_dict["loader_label"].append("C2引擎"+":"+behavior_map_dict.get(each_d.get("malname","").split("/")[0].lower(),"")) 

                    #丰富扩展详情中的载荷详情数据
                    loader_info_dict["v_name"] = each_d.get("malname")
                    loader_info_dict["v_type"] = each_d.get("virus_type")
                    loader_info_dict["v_family"] = each_d.get("virus_family")
                    loader_info_dict["platform"] = each_d.get("virus_platform")
                    loader_info_dict["attack_action"] = each_d.get("virus_behaviors")
                    eventlog_template_dict["threat_info"]["extend_details"]["loader_info"].append(loader_info_dict)
                    eventlog_template_dict["threat_info"]["source_load"].append(d_dict) 
                                     
            if each_dict.get("d",{}).get("sengine"):
                d_have_load_sign = True
                for each_d in each_dict.get("d",{}).get("sengine"):
                    d_dict = copy.deepcopy(eventlog_template_dict["threat_info"]["source_load"][0])  #加载源载荷中的数据模板
                    loader_info_dict = copy.deepcopy(eventlog_template_dict["threat_info"]["extend_details"]["loader_info"][0])  #加载扩展详情中的载荷详情数据模板
                    eventlog_template_dict["Used_by_ES_src"]["source_load_attack_action"] = each_d.get("virus_behaviors")  # 给ES提供的数据
                    eventlog_template_dict["Used_by_ES_src"]["source_load_attack_v_name"] = each_d.get("malname")          # 给ES提供的数据
                    eventlog_template_dict["Used_by_ES_src"]["source_load_attack_v_type"] = each_d.get("virus_type")         # 给ES提供的数据
                    eventlog_template_dict["Used_by_ES_src"]["source_load_attack_v_family"] = each_d.get("virus_family")         # 给ES提供的数据
                    # 判断malname中有没有关于行为的描述即(“[行为]”)
                    if each_d.get("malname","").split("[")[0] == each_d.get("malname",""):   # malname中没有关于[载荷行为]的信息
                        if behavior_map_dict.get(each_d.get("malname","").split("/")[0].lower(),"") != "":
                            sengine_label = behavior_map_dict.get(each_d.get("malname","").split("/")[0].lower(),"")                           
                            eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_label"].append("sengine"+":"+sengine_label)
                            eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_name"].append("sengine"+":"+sengine_label)
                    else:
                        eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_label"].append("sengine"+":"+behavior_map_dict.get(each_d.get("malname","").split("[")[0].lower(),""))  
                        eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_name"].append("sengine"+":"+behavior_map_dict.get(each_d.get("malname","").split("[")[0].lower(),""))  
                        # 提取malname中关于[载荷行为]的信息
                        sengine_xw_label = behavior_map_dict.get(re.findall(r'[[](.*?)[]]', each_d.get("malname"))[0].lower(),"")
                        if not sengine_xw_label == "":   # 如果在载荷行为映射表map.json中匹配到了则添加进loader_behavior_label 否则为空列表
                            d_dict["loader_behavior_label"].append("sengine"+":"+sengine_xw_label)   #sengine没有载荷标签 但有载荷行为标签
                    d_dict["v_name"] = each_d.get("malname")
                    d_dict["v_type"] = each_d.get("virus_type")
                    d_dict["v_family"] = each_d.get("virus_family")
                    d_dict["platform"] = each_d.get("virus_platform")
                    d_dict["attack_action"] = each_d.get("virus_behaviors")
                    if each_d.get("nsacss"):
                        nsacss_info_number={
                            "Administer": "1",
                            "Engagement": "2",
                            "Installation|Execution": "3",
                            "Installation & Execution":"3",
                            "Installation":"3",
                            "Execution":"3",
                            "Privilege Escalation":"3",
                            "Credential Access": "4",
                            "Lateral Movement": "4",
                            "Persistence": "4",
                            "Effect": "5",
                            "Ongoing Processes": "6"
                        }                        
                        for nsacss_i in each_d.get("nsacss"):
                            if nsacss_i.get("stage") in nsacss_info_number:
                                nsacss_i["stage_number"] = nsacss_info_number[nsacss_i.get("stage")]
                            if nsacss_i.get("stage") == "Presence":   # 因在nsacss表的阶段中有两个presence（阶段相同 目标不相同所以需进一步判断objective）
                                nsacss_i["stage_number"] = nsacss_info_number[nsacss_i.get("objective")]
                            eventlog_template_dict["threat_info"]["sponsor_behavior"]["attack_label"]["nsacss"].append(nsacss_i)
                        del eventlog_template_dict["threat_info"]["sponsor_behavior"]["attack_label"]["nsacss"][0] #删除模板中原始模板数据
                    if each_d.get("attck"):
                        attck_info_number={
                            "Initial Access": "1",
                            "Execution": "2",
                            "Persistence": "3",
                            "Privilege Escalation": "4",
                            "Defense Evasion": "5",
                            "Credential Access": "6",
                            "Discovery": "7",
                            "Lateral Movement": "8",
                            "Collection": "9",
                            "Command and Control": "10",
                            "Command And Control": "10",
                            "Exfiltration": "11",
                            "Impact": "12"
                        } 
                        for attck_i in each_d.get("attck"):
                            # 从PG中获取attck技术编号
                            techniques_i = attck_i.get("techniques")
                            cur_attck = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                            cur_attck.execute("SELECT attck_id FROM k_attck WHERE attck_eng_name=%s",(techniques_i,))
                            techniques_result = cur_attck.fetchone()
                            if not techniques_result:                               
                                cur_attck.execute("insert into k_attck(attck_id,attck_eng_name) values((SELECT MAX(attck_id) FROM k_attck)+1,%s) RETURNING attck_id",(techniques_i,))
                                techniques_num = cur_attck.fetchone()[0]
                            else:
                                techniques_num = techniques_result[0]
                            conn.commit()
                            cur_attck.close()                               
                            # ================================================================
                            # 处理PTD对attack上报格式不规范的问题，当tatic字段中出现如"Defense Evasion, Execution"多种战术时，将其拆开
                            if "," in attck_i.get("tactic"):
                                tactic_list = attck_i.get("tactic").split(",")
                                for tactic_list_i in tactic_list:
                                    attck_i_dict = dict()
                                    if tactic_list_i[0] == " ":  # 如果split后有字符串以空格开头则把空格删掉
                                        tactic_list_i = tactic_list_i.replace(" ","",1)  # 将字符串中的空格替换掉并最多替换1次
                                    attck_i_dict["tactic"] = tactic_list_i
                                    attck_i_dict["techniques"] = techniques_i
                                    attck_i_dict["tactic_number"] = attck_info_number[tactic_list_i]
                                    attck_i_dict["techniques_number"] = str(techniques_num)   
                                    eventlog_template_dict["threat_info"]["sponsor_behavior"]["attack_label"]["attack"].append(attck_i_dict)                                
                            else:
                                attck_i["tactic_number"] = attck_info_number[attck_i.get("tactic")]
                                attck_i["techniques_number"] = str(techniques_num) 
                                eventlog_template_dict["threat_info"]["sponsor_behavior"]["attack_label"]["attack"].append(attck_i)
                            # ================================================================
                        del eventlog_template_dict["threat_info"]["sponsor_behavior"]["attack_label"]["attack"][0] #删除模板中原始模板数据
                    if each_d.get("reference"):
                        for reference_info in each_d.get("reference"):
                            reference_info = reference_info.split(",")
                            if reference_info[0] == "cve":
                                vulne_dict = copy.deepcopy(eventlog_template_dict["threat_info"]["purpose_load"]["vulne"][0])  #加载源载荷中的关于vulne的数据模板
                                vulne_dict["vulne_id"] = reference_info[1]
                                d_dict["vulne"].append(vulne_dict)
                        if len(d_dict["vulne"]) >= 2:   # 证明有cve信息添加进了vulne列表，则删除[0]模板数据
                            del d_dict["vulne"][0]            
                    d_dict["detect"]["detect_time"] = each_dict["ts"]["start"]
                    d_dict["detect"]["detect_pro"] = "PTD"
                    d_dict["detect"]["detect_pro_id"] = each_dict.get("dev")
                    d_dict["detect"]["detect_rule"]["detect_base"] = each_d.get("author","") 
                    d_dict["detect"]["detect_rule"]["detect_id"] = each_d.get("rid","")
                    # d_dict["loader_label"].append("sengine"+":"+behavior_map_dict[each_d.get("malname","").split("/")[0].lower()])   sengine没有载荷标签

                    #丰富扩展详情中的载荷详情数据
                    loader_info_dict["v_name"] = each_d.get("malname")
                    loader_info_dict["v_type"] = each_d.get("virus_type")
                    loader_info_dict["v_family"] = each_d.get("virus_family")
                    loader_info_dict["platform"] = each_d.get("virus_platform")
                    loader_info_dict["attack_action"] = each_d.get("virus_behaviors")
                    eventlog_template_dict["threat_info"]["extend_details"]["loader_info"].append(loader_info_dict)
                    eventlog_template_dict["threat_info"]["source_load"].append(d_dict)

# ==========================2020.2.21修改==============================================
            if each_dict.get("d",{}).get("file"):
                d_have_load_sign = True
                for each_d in each_dict.get("d",{}).get("file"):
                    d_dict = copy.deepcopy(eventlog_template_dict["threat_info"]["source_load"][0])  #加载源载荷中的数据模板
                    loader_info_dict = copy.deepcopy(eventlog_template_dict["threat_info"]["extend_details"]["loader_info"][0])  #加载扩展详情中的载荷详情数据模板
                    # 从ptd的file字段中解析到的文件信息
                    if md5_and_filedata:
                        file_info_dict = md5_and_filedata.get(each_d.get("md5"))  # 根据d.file中的md5匹配外层file字段的文件数据
                        if file_info_dict:
                            eventlog_template_dict["Used_by_ES_src"]["source_load_attack_action"] = each_d["virus_behaviors"]  # 给ES提供的数据
                            eventlog_template_dict["Used_by_ES_src"]["source_load_attack_v_name"] = each_d["malname"]          # 给ES提供的数据
                            eventlog_template_dict["Used_by_ES_src"]["source_load_attack_v_type"] = each_d.get("virus_type")         # 给ES提供的数据
                            eventlog_template_dict["Used_by_ES_src"]["source_load_attack_v_family"] = each_d.get("virus_family")         # 给ES提供的数据                        
                            
                            loader_info_dict["o_filename"] = urllib.parse.unquote(file_info_dict.get("name",""))
                            loader_info_dict["size"] = file_info_dict.get("bytes","")
                            loader_info_dict["file_format"] = file_info_dict.get("type","")                    
                            d_dict["size"] = file_info_dict.get("bytes","")
                            d_dict["o_filename"] = urllib.parse.unquote(file_info_dict.get("name",""))
                            d_dict["file_format"] = file_info_dict.get("type","")
                            if file_info_dict.get("direction") == "up":                           
                                eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_label"].append("AVL引擎:上传文件")
                                eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_name"].append("AVL引擎:上传文件")
                            if file_info_dict.get("direction") == "down":
                                eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_label"].append("AVL引擎:下载文件")
                                eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_name"].append("AVL引擎:下载文件")                        
                            d_dict["v_name"] = each_d.get("malname")
                            d_dict["v_type"] = each_d.get("virus_type")
                            d_dict["v_family"] = each_d.get("virus_family")
                            d_dict["platform"] = each_d.get("virus_platform")
                            d_dict["attack_action"] = each_d.get("virus_behaviors")
                            d_dict["fprocess_hash"]["md5"] = each_d.get("md5")
                            d_dict["detect"]["detect_time"] = each_dict.get("ts",{}).get("start")
                            d_dict["detect"]["detect_pro"] = "PTD"
                            d_dict["detect"]["detect_pro_id"] = each_dict.get("dev")
                            d_dict["detect"]["detect_rule"]["detect_base"] = each_d.get("author","") 
                            d_dict["detect"]["detect_rule"]["detect_id"] = each_d.get("rid","")
                            # 判断malname中有没有关于行为的描述即(“[载荷行为]”)
                            if each_d.get("malname","").split("[")[0] == each_d.get("malname",""):  # 没有匹配到[载荷行为]
                                if behavior_map_dict.get(each_d.get("malname","").split("/")[0].lower(),"") != "":                           
                                    d_dict["loader_label"].append("AVL引擎"+":"+behavior_map_dict.get(each_d.get("malname","").split("/")[0].lower(),""))
                            else:
                                d_dict["loader_label"].append("AVL引擎"+":"+behavior_map_dict.get(each_d.get("malname","").split("[")[0].lower(),""))  
                                file_xw_label = behavior_map_dict.get(re.findall(r'[[](.*?)[]]', each_d.get("malname"))[0].lower(),"")
                                if not file_xw_label == "":   # 如果在载荷行为映射表map.json中匹配到了则添加进loader_behavior_label 否则为空列表
                                    d_dict["loader_behavior_label"].append("AVL引擎"+":"+file_xw_label)                     
                            #丰富扩展详情中的载荷详情数据
                            loader_info_dict["v_name"] = each_d.get("malname")
                            loader_info_dict["v_type"] = each_d.get("virus_type")
                            loader_info_dict["v_family"] = each_d.get("virus_family")
                            loader_info_dict["platform"] = each_d.get("virus_platform")
                            loader_info_dict["attack_action"] = each_d.get("virus_behaviors")
                            loader_info_dict["file_hash"]["md5"] = each_d.get("md5")
                            eventlog_template_dict["threat_info"]["extend_details"]["loader_info"].append(loader_info_dict)
                            eventlog_template_dict["threat_info"]["source_load"].append(d_dict)                   
                    # 在外层的file字段为null
                    else:
                        eventlog_template_dict["Used_by_ES_src"]["source_load_attack_action"] = each_d["virus_behaviors"]  # 给ES提供的数据
                        eventlog_template_dict["Used_by_ES_src"]["source_load_attack_v_name"] = each_d["malname"]          # 给ES提供的数据
                        eventlog_template_dict["Used_by_ES_src"]["source_load_attack_v_type"] = each_d.get("virus_type")         # 给ES提供的数据
                        eventlog_template_dict["Used_by_ES_src"]["source_load_attack_v_family"] = each_d.get("virus_family")         # 给ES提供的数据

                        d_dict["v_name"] = each_d.get("malname")
                        d_dict["v_type"] = each_d.get("virus_type")
                        d_dict["v_family"] = each_d.get("virus_family")
                        d_dict["platform"] = each_d.get("virus_platform")
                        d_dict["attack_action"] = each_d.get("virus_behaviors")
                        d_dict["fprocess_hash"]["md5"] = each_d.get("md5")
                        d_dict["detect"]["detect_time"] = each_dict.get("ts",{}).get("start")
                        d_dict["detect"]["detect_pro"] = "PTD"
                        d_dict["detect"]["detect_pro_id"] = each_dict.get("dev")
                        d_dict["detect"]["detect_rule"]["detect_base"] = each_d.get("author","") 
                        d_dict["detect"]["detect_rule"]["detect_id"] = each_d.get("rid","")
                        # 判断malname中有没有关于行为的描述即(“[载荷行为]”)
                        if each_d.get("malname","").split("[")[0] == each_d.get("malname",""):  # 没有匹配到[载荷行为]
                            if behavior_map_dict.get(each_d.get("malname","").split("/")[0].lower(),"") != "":                           
                                d_dict["loader_label"].append("AVL引擎"+":"+behavior_map_dict.get(each_d.get("malname","").split("/")[0].lower(),""))
                        else:
                            d_dict["loader_label"].append("AVL引擎"+":"+behavior_map_dict.get(each_d.get("malname","").split("[")[0].lower(),""))  
                            file_xw_label = behavior_map_dict.get(re.findall(r'[[](.*?)[]]', each_d.get("malname"))[0].lower(),"")
                            if not file_xw_label == "":   # 如果在载荷行为映射表map.json中匹配到了则添加进loader_behavior_label 否则为空列表                               
                                d_dict["loader_behavior_label"].append("AVL引擎"+":"+file_xw_label)                     
                        #丰富扩展详情中的载荷详情数据
                        loader_info_dict["v_name"] = each_d.get("malname")
                        loader_info_dict["v_type"] = each_d.get("virus_type")
                        loader_info_dict["v_family"] = each_d.get("virus_family")
                        loader_info_dict["platform"] = each_d.get("virus_platform")
                        loader_info_dict["attack_action"] = each_d.get("virus_behaviors")
                        loader_info_dict["file_hash"]["md5"] = each_d.get("md5")
                        eventlog_template_dict["threat_info"]["extend_details"]["loader_info"].append(loader_info_dict)
                        eventlog_template_dict["threat_info"]["source_load"].append(d_dict)                        

            # 当外层file字段有值但d.file没有值时
            if not each_dict.get("d",{}).get("file") and md5_and_filedata:
                d_have_load_sign = True
                # 从ptd的file字段中解析到的文件信息
                md5_list_from_file = md5_and_filedata.keys()
                for md5_i in md5_list_from_file:
                    d_dict = copy.deepcopy(eventlog_template_dict["threat_info"]["source_load"][0])  #加载源载荷中的数据模板
                    loader_info_dict = copy.deepcopy(eventlog_template_dict["threat_info"]["extend_details"]["loader_info"][0])  #加载扩展详情中的载荷详情数据模板
                    file_info_dict = md5_and_filedata.get(md5_i)  # 根据d.file中的md5匹配外层file字段的文件数据
                    if file_info_dict:
                        loader_info_dict["o_filename"] = urllib.parse.unquote(file_info_dict.get("name",""))
                        loader_info_dict["size"] = file_info_dict.get("bytes","")
                        loader_info_dict["file_format"] = file_info_dict.get("type","")                    
                        d_dict["size"] = file_info_dict.get("bytes","")
                        d_dict["o_filename"] = urllib.parse.unquote(file_info_dict.get("name",""))
                        d_dict["file_format"] = file_info_dict.get("type","")
                        d_dict["fprocess_hash"]["md5"] = file_info_dict.get("md5","")
                        if file_info_dict.get("direction") == "up":                           
                            eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_label"].append("AVL引擎:上传文件")
                            eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_name"].append("AVL引擎:上传文件")
                        if file_info_dict.get("direction") == "down":
                            eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_label"].append("AVL引擎:下载文件")
                            eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_name"].append("AVL引擎:下载文件")                        

                        d_dict["detect"]["detect_time"] = each_dict.get("ts",{}).get("start")
                        d_dict["detect"]["detect_pro"] = "PTD"
                        d_dict["detect"]["detect_pro_id"] = each_dict.get("dev")                  

                        eventlog_template_dict["threat_info"]["extend_details"]["loader_info"].append(loader_info_dict)
                        eventlog_template_dict["threat_info"]["source_load"].append(d_dict)

            if each_dict.get("d",{}).get("domain"):
                eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_label"].append("C2引擎:C2通信")
                eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_name"].append("C2引擎:C2通信")                
                d_have_load_sign = True
                for each_d in each_dict.get("d",{}).get("domain"):
                    d_dict = copy.deepcopy(eventlog_template_dict["threat_info"]["source_load"][0])  #加载源载荷中的数据模板
                    loader_info_dict = copy.deepcopy(eventlog_template_dict["threat_info"]["extend_details"]["loader_info"][0])  #加载扩展详情中的载荷详情数据模板
                    eventlog_template_dict["Used_by_ES_src"]["source_load_attack_action"] = each_d.get("virus_behaviors")  # 给ES提供的数据
                    eventlog_template_dict["Used_by_ES_src"]["source_load_attack_v_name"] = each_d.get("malname")          # 给ES提供的数据
                    eventlog_template_dict["Used_by_ES_src"]["source_load_attack_v_type"] = each_d.get("virus_type")         # 给ES提供的数据
                    eventlog_template_dict["Used_by_ES_src"]["source_load_attack_v_family"] = each_d.get("virus_family")         # 给ES提供的数据

                    d_dict["v_name"] = each_d.get("malname")
                    d_dict["v_type"] = each_d.get("virus_type")
                    d_dict["v_family"] = each_d.get("virus_family")
                    d_dict["platform"] = each_d.get("virus_platform")
                    d_dict["attack_action"] = each_d.get("virus_behaviors")
                    d_dict["detect"]["detect_time"] = each_dict.get("ts",{}).get("start")
                    d_dict["detect"]["detect_pro"] = "PTD"
                    d_dict["detect"]["detect_pro_id"] = each_dict.get("dev")
                    d_dict["detect"]["detect_rule"]["detect_base"] = each_d.get("author","") 
                    d_dict["detect"]["detect_rule"]["detect_id"] = each_d.get("rid","") 
                    d_dict["loader_label"].append("C2引擎"+":"+behavior_map_dict.get(each_d.get("malname","").split("/")[0].lower(),""))
                    #丰富扩展详情中的载荷详情数据
                    loader_info_dict["v_name"] = each_d.get("malname")
                    loader_info_dict["v_type"] = each_d.get("virus_type")
                    loader_info_dict["v_family"] = each_d.get("virus_family")
                    loader_info_dict["platform"] = each_d.get("virus_platform")
                    loader_info_dict["attack_action"] = each_d.get("virus_behaviors")
                    eventlog_template_dict["threat_info"]["extend_details"]["loader_info"].append(loader_info_dict)
                    eventlog_template_dict["threat_info"]["source_load"].append(d_dict)

            if each_dict.get("d",{}).get("ip"):
                eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_label"].append("C2引擎:C2通信")
                eventlog_template_dict["threat_info"]["sponsor_behavior"]["xw_name"].append("C2引擎:C2通信")  
                d_have_load_sign = True
                for each_d in each_dict.get("d",{}).get("ip"):
                    d_dict = copy.deepcopy(eventlog_template_dict["threat_info"]["source_load"][0])  #加载源载荷中的数据模板
                    loader_info_dict = copy.deepcopy(eventlog_template_dict["threat_info"]["extend_details"]["loader_info"][0])  #加载扩展详情中的载荷详情数据模板
                    eventlog_template_dict["Used_by_ES_src"]["source_load_attack_action"] = each_d.get("virus_behaviors")  # 给ES提供的数据
                    eventlog_template_dict["Used_by_ES_src"]["source_load_attack_v_name"] = each_d.get("malname")          # 给ES提供的数据
                    eventlog_template_dict["Used_by_ES_src"]["source_load_attack_v_type"] = each_d.get("virus_type")         # 给ES提供的数据
                    eventlog_template_dict["Used_by_ES_src"]["source_load_attack_v_family"] = each_d.get("virus_family")         # 给ES提供的数据

                    d_dict["v_name"] = each_d.get("malname")
                    d_dict["v_type"] = each_d.get("virus_type")
                    d_dict["v_family"] = each_d.get("virus_family")
                    d_dict["platform"] = each_d.get("virus_platform")
                    d_dict["attack_action"] = each_d.get("virus_behaviors")
                    d_dict["detect"]["detect_time"] = each_dict.get("ts",{}).get("start")
                    d_dict["detect"]["detect_pro"] = "PTD"
                    d_dict["detect"]["detect_pro_id"] = each_dict.get("dev")
                    d_dict["detect"]["detect_rule"]["detect_base"] = each_d.get("author","") 
                    d_dict["detect"]["detect_rule"]["detect_id"] = each_d.get("rid","")
                    d_dict["loader_label"].append("C2引擎"+":"+behavior_map_dict.get(each_d.get("malname","").split("/")[0].lower(),"")) 
                    #丰富扩展详情中的载荷详情数据
                    loader_info_dict["v_name"] = each_d.get("malname")
                    loader_info_dict["v_type"] = each_d.get("virus_type")
                    loader_info_dict["v_family"] = each_d.get("virus_family")
                    loader_info_dict["platform"] = each_d.get("virus_platform")
                    loader_info_dict["attack_action"] = each_d.get("virus_behaviors")
                    eventlog_template_dict["threat_info"]["extend_details"]["loader_info"].append(loader_info_dict)
                    eventlog_template_dict["threat_info"]["source_load"].append(d_dict)

            if each_dict.get("d",{}).get("cengine"):    # cengine 暂不处理 文档缺少字段描述信息
                pass
            if d_have_load_sign :   # 如果d_have_load_sign为True 则说明已经提取了d字段下关于载荷的信息并添加到了eventlog对应位置
                del eventlog_template_dict["threat_info"]["extend_details"]["loader_info"][0] # 删除模板中的空数据
                del eventlog_template_dict["threat_info"]["source_load"][0]   # 删除模板中的空数据

            eventlog_template_dict["threat_info"]["sponsor_time"] = each_dict.get("ts",{}).get("start")
            eventlog_template_dict["threat_info"]["source_endpoint"]["ip_info"]["ip"] = each_dict.get("src",{}).get("ip")
            eventlog_template_dict["threat_info"]["source_endpoint"]["ip_info"]["port"] = each_dict.get("src",{}).get("port")
            eventlog_template_dict["threat_info"]["source_endpoint"]["ip_info"]["mac"] = each_dict.get("src",{}).get("mac")
            eventlog_template_dict["threat_info"]["source_endpoint"]["ip_info"]["ipv6"] = each_dict.get("src",{}).get("ipv6")
            eventlog_template_dict["threat_info"]["source_endpoint"]["ip_info"]["ip_country"] = each_dict.get("src",{}).get("country")
            eventlog_template_dict["threat_info"]["source_endpoint"]["ip_info"]["ip_city"] = each_dict.get("src",{}).get("city")
            eventlog_template_dict["threat_info"]["source_endpoint"]["ip_info"]["ip_region"] = each_dict.get("src",{}).get("region")
            # eventlog_template_dict["threat_info"]["source_endpoint"]["ip_info"]["ip_cp"] = each_dict.get("src",{}).get("unit")
            eventlog_template_dict["threat_info"]["source_endpoint"]["ip_info"]["isp"] = each_dict.get("src",{}).get("isp")
            eventlog_template_dict["threat_info"]["source_endpoint"]["ip_info"]["ip_longitude"] = each_dict["src"].get("location",{}).get("lon","")
            eventlog_template_dict["threat_info"]["source_endpoint"]["ip_info"]["ip_longitude"] = each_dict["src"].get("location",{}).get("lat","")

            # eventlog_template_dict["threat_info"]["source_endpoint"]["url"] = urllib.parse.unquote(list_null(each_dict.get("url")))
            # eventlog_template_dict["threat_info"]["source_endpoint"]["domain_info"]["domain"] = list_null(each_dict.get("domain")) 
            # ---------------------当原始数据中url数据但domain中没有数据时，截取url中的domain添加到模板对应位置中---------------------
            # if eventlog_template_dict["threat_info"]["source_endpoint"]["domain_info"]["domain"] == "" and not eventlog_template_dict["threat_info"]["source_endpoint"]["url"] == "":
            #     eventlog_template_dict["threat_info"]["source_endpoint"]["domain_info"]["domain"] = urlparse(eventlog_template_dict["threat_info"]["source_endpoint"]["url"]).netloc 
            eventlog_template_dict["threat_info"]["source_endpoint"]["label"] = each_dict.get("label",[])
            eventlog_template_dict["threat_info"]["source_endpoint"]["detect"]["detect_time"] = each_dict.get("ts",{}).get("start")
            eventlog_template_dict["threat_info"]["source_endpoint"]["detect"]["detect_pro"] = "PTD"
            eventlog_template_dict["threat_info"]["source_endpoint"]["detect"]["detect_pro_id"] = each_dict.get("dev")

            # 源端点的关联资产数据
            # 判断ip是否为内部网络或外部资产ip,若是则调用注册资产的接口，若不是则略过
            if check_private_addr(each_dict["src"]["ip"]) == 2 or External_Asset(each_dict["src"]["ip"]) == "waibuzichan":
                cur_s = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # 不论是不是内网扫描产生的数据src端都应调用资产丰富的函数（dst端需要判断s2c字段中发包量是否为0以确定是不是内网扫描产出的数据）
                src_asset_info_list = get_asset_data(each_dict["src"]["ip"],each_dict["src"]["mac"],each_dict.get("dev"),hbase_rowkey,cur_s,conn)
                if isinstance(src_asset_info_list,list):
                    logger.info("src_uuid is %s"%(src_asset_info_list[0]))
                    eventlog_template_dict["threat_info"]["source_endpoint"]["asset_detail"]["asset_id"] = src_asset_info_list[0]
                    eventlog_template_dict["threat_info"]["source_endpoint"]["asset_detail"]["os"] = src_asset_info_list[1]
                    for src_asset_info_list_ipmac in src_asset_info_list[2]:
                        src_ipmac_dict = {}
                        src_ipmac_dict["ip"] = src_asset_info_list_ipmac[0]
                        src_ipmac_dict["mac"] = src_asset_info_list_ipmac[1]
                        eventlog_template_dict["threat_info"]["source_endpoint"]["asset_detail"]["ip_group"].append(src_ipmac_dict)
                    del eventlog_template_dict["threat_info"]["source_endpoint"]["asset_detail"]["ip_group"][0]                    
                    eventlog_template_dict["threat_info"]["source_endpoint"]["asset_detail"]["unit"] = str_null(src_asset_info_list[3])
                    eventlog_template_dict["threat_info"]["source_endpoint"]["asset_detail"]["network_struct"] = src_asset_info_list[4]
                    eventlog_template_dict["threat_info"]["source_endpoint"]["asset_detail"]["type"] = src_asset_info_list[5]
                    eventlog_template_dict["threat_info"]["source_endpoint"]["asset_detail"]["asset_level"] = src_asset_info_list[6]
                    eventlog_template_dict["threat_info"]["source_endpoint"]["asset_detail"]["people"]["user"] = src_asset_info_list[7]
                    eventlog_template_dict["threat_info"]["source_endpoint"]["asset_detail"]["people"]["manager"] = src_asset_info_list[8]
                    eventlog_template_dict["threat_info"]["source_endpoint"]["asset_detail"]["applications"] = src_asset_info_list[9]
                    # 端点所属单位的信息 先遵从pg库中查到的 如果没有则遵循ptd的unit字段给出的-----2020-1-14修改------
                    if src_asset_info_list[3]:    # 如果在pg中查到了所属单位名
                        eventlog_template_dict["Used_by_ES_src"]["source_endpoint_ip_cp"] = src_asset_info_list[3]
                        eventlog_template_dict["threat_info"]["source_endpoint"]["ip_info"]["ip_cp"] = src_asset_info_list[3]
                    else:
                        eventlog_template_dict["Used_by_ES_src"]["source_endpoint_ip_cp"] = each_dict.get("src",{}).get("unit")
                        eventlog_template_dict["threat_info"]["source_endpoint"]["ip_info"]["ip_cp"] = each_dict.get("src",{}).get("unit")
            else:
                src_asset_info_list = None

            # 丰富载体详细信息
            if each_dict.get("dns"):
                ptd_protocol = "dns"
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["dns_info"]["message_type"] = each_dict.get("dns",{}).get("msg",{}).get("msg_type","")
                # ptd中的规则dns.msg.msg_type为query时，域名和url归属于目的 （2020.1.13增加）
                if each_dict.get("dns",{}).get("msg",{}).get("msg_type") == "query":
                    if attacker_change_flag:    # 判断是否做了src和dst的互换
                        eventlog_template_dict["threat_info"]["source_endpoint"]["url"] = urllib.parse.unquote(list_null(each_dict.get("url")))
                        eventlog_template_dict["threat_info"]["source_endpoint"]["domain_info"]["domain"] = list_null(each_dict.get("domain"))
                        eventlog_template_dict["Used_by_ES_src"]["source_endpoint_url"] = list_null(each_dict.get("url"))   
                        eventlog_template_dict["Used_by_ES_src"]["source_endpoint_domain"] = list_null(each_dict.get("domain"))
                        #---------------------当原始数据中url数据但domain中没有数据时，截取url中的domain添加到模板对应位置中---------------------
                        if eventlog_template_dict["threat_info"]["source_endpoint"]["domain_info"]["domain"] == "" and not eventlog_template_dict["threat_info"]["source_endpoint"]["url"] == "":
                            domain_op = urlparse(eventlog_template_dict["threat_info"]["source_endpoint"]["url"]).netloc
                            eventlog_template_dict["threat_info"]["source_endpoint"]["domain_info"]["domain"] = domain_op
                            eventlog_template_dict["Used_by_ES_src"]["source_endpoint_domain"] = domain_op    
                    else:
                        eventlog_template_dict["threat_info"]["purpose_endpoint"]["url"] = urllib.parse.unquote(list_null(each_dict.get("url")))
                        eventlog_template_dict["threat_info"]["purpose_endpoint"]["domain_info"]["domain"] = list_null(each_dict.get("domain")) 
                        eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_url"] = list_null(each_dict.get("url"))
                        eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_domain"] = list_null(each_dict.get("domain"))
                        #---------------------当原始数据中url数据但domain中没有数据时，截取url中的domain添加到模板对应位置中---------------------
                        if eventlog_template_dict["threat_info"]["purpose_endpoint"]["domain_info"]["domain"] == "" and not eventlog_template_dict["threat_info"]["purpose_endpoint"]["url"] == "":
                            domain_op = urlparse(eventlog_template_dict["threat_info"]["purpose_endpoint"]["url"]).netloc
                            eventlog_template_dict["threat_info"]["purpose_endpoint"]["domain_info"]["domain"] = domain_op
                            eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_domain"] = domain_op    
                else:
                    if attacker_change_flag:
                        eventlog_template_dict["threat_info"]["purpose_endpoint"]["url"] = urllib.parse.unquote(list_null(each_dict.get("url")))
                        eventlog_template_dict["threat_info"]["purpose_endpoint"]["domain_info"]["domain"] = list_null(each_dict.get("domain")) 
                        eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_url"] = list_null(each_dict.get("url"))
                        eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_domain"] = list_null(each_dict.get("domain"))
                        #---------------------当原始数据中url数据但domain中没有数据时，截取url中的domain添加到模板对应位置中---------------------
                        if eventlog_template_dict["threat_info"]["purpose_endpoint"]["domain_info"]["domain"] == "" and not eventlog_template_dict["threat_info"]["purpose_endpoint"]["url"] == "":
                            domain_op = urlparse(eventlog_template_dict["threat_info"]["purpose_endpoint"]["url"]).netloc
                            eventlog_template_dict["threat_info"]["purpose_endpoint"]["domain_info"]["domain"] = domain_op
                            eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_domain"] = domain_op                             
                    else:
                        eventlog_template_dict["threat_info"]["source_endpoint"]["url"] = urllib.parse.unquote(list_null(each_dict.get("url")))
                        eventlog_template_dict["threat_info"]["source_endpoint"]["domain_info"]["domain"] = list_null(each_dict.get("domain"))
                        eventlog_template_dict["Used_by_ES_src"]["source_endpoint_url"] = list_null(each_dict.get("url"))   
                        eventlog_template_dict["Used_by_ES_src"]["source_endpoint_domain"] = list_null(each_dict.get("domain"))
                        #---------------------当原始数据中url数据但domain中没有数据时，截取url中的domain添加到模板对应位置中---------------------
                        if eventlog_template_dict["threat_info"]["source_endpoint"]["domain_info"]["domain"] == "" and not eventlog_template_dict["threat_info"]["source_endpoint"]["url"] == "":
                            domain_op = urlparse(eventlog_template_dict["threat_info"]["source_endpoint"]["url"]).netloc
                            eventlog_template_dict["threat_info"]["source_endpoint"]["domain_info"]["domain"] = domain_op
                            eventlog_template_dict["Used_by_ES_src"]["source_endpoint_domain"] = domain_op
                try:
                    eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["dns_info"]["record"]["record_type"] = each_dict.get("dns",{}).get("msg",{}).get("record",[{}])[0].get("Rtype","")
                    eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["dns_info"]["record"]["pro_info"] = each_dict.get("dns",{}).get("msg",{}).get("record",[{}])[0].get("class","")
                    eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["dns_info"]["record"]["alias"] = each_dict.get("dns",{}).get("msg",{}).get("record",[{}])[0].get("cname","")
                    eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["dns_info"]["record"]["return_data"] = each_dict.get("dns",{}).get("msg",{}).get("record",[{}])[0].get("data","")
                except:
                    pass
            if each_dict.get("email"):
                ptd_protocol = "email"
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["email_info"]["from"] = each_dict.get("email",{}).get("msg",{}).get("from","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["email_info"]["to"] = list_null(each_dict.get("email",{}).get("msg",{}).get("to"))
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["email_info"]["sender"] = each_dict.get("email",{}).get("msg",{}).get("sender","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["email_info"]["sub"] = each_dict.get("email",{}).get("msg",{}).get("sub","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["email_info"]["content"]["text"] = each_dict.get("email",{}).get("msg",{}).get("content","")
                
                for each_attach in list_dict_null(each_dict.get("email",{}).get("msg",{}).get("attach")):
                    attach_dict = copy.deepcopy(eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["email_info"]["file"][0])
                    attach_dict["filehash"]["md5"] = each_attach.get("md5","")
                    attach_dict["file_name"] = each_attach.get("fn","")
                    attach_dict["file_format"] = each_attach.get("ft","")
                    eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["email_info"]["file"].append(attach_dict)
                # 将解析出来的数据填到eventlog模板中的对应位置，并删除eventlog模板中原有的模板空数据 
                del eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["email_info"]["file"][0]   
            if each_dict.get("ftp"):
                ptd_protocol = "ftp"
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["ftp_info"]["upload"]["bytes"] = str(each_dict.get("ftp",{}).get("download",{}).get("bytes",""))
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["ftp_info"]["upload"]["direct"] = each_dict.get("ftp",{}).get("download",{}).get("direction","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["ftp_info"]["upload"]["file_name"] = each_dict.get("ftp",{}).get("download",{}).get("fn","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["ftp_info"]["upload"]["file_type"] = each_dict.get("ftp",{}).get("download",{}).get("ft","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["ftp_info"]["upload"]["file_hash"]["md5"] = each_dict.get("ftp",{}).get("download",{}).get("md5","")
            if each_dict.get("http"):
                ptd_protocol = "http"
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["request"]["header"]["method"] = each_dict.get("http",{}).get("request",{}).get("method","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["request"]["header"]["URL"] = each_dict.get("http",{}).get("request",{}).get("header",{}).get("host","") + each_dict.get("http",{}).get("request",{}).get("header",{}).get("uri","")
                # 判断是否存在src和dst得互换，如果换了则ptd中url和domain归属于源，否则归属于目的
                if attacker_change_flag:
                    eventlog_template_dict["threat_info"]["source_endpoint"]["url"] = urllib.parse.unquote(list_null(each_dict.get("url")))
                    eventlog_template_dict["threat_info"]["source_endpoint"]["domain_info"]["domain"] = list_null(each_dict.get("domain"))
                    eventlog_template_dict["Used_by_ES_src"]["source_endpoint_url"] = list_null(each_dict.get("url"))   
                    eventlog_template_dict["Used_by_ES_src"]["source_endpoint_domain"] = list_null(each_dict.get("domain"))
                    #---------------------当原始数据中url数据但domain中没有数据时，截取url中的domain添加到模板对应位置中---------------------
                    if eventlog_template_dict["threat_info"]["source_endpoint"]["domain_info"]["domain"] == "" and not eventlog_template_dict["threat_info"]["source_endpoint"]["url"] == "":
                        domain_op = urlparse(eventlog_template_dict["threat_info"]["source_endpoint"]["url"]).netloc
                        eventlog_template_dict["threat_info"]["source_endpoint"]["domain_info"]["domain"] = domain_op
                        eventlog_template_dict["Used_by_ES_src"]["source_endpoint_domain"] = domain_op    
                else:
                    eventlog_template_dict["threat_info"]["purpose_endpoint"]["url"] = urllib.parse.unquote(list_null(each_dict.get("url")))
                    eventlog_template_dict["threat_info"]["purpose_endpoint"]["domain_info"]["domain"] = list_null(each_dict.get("domain")) 
                    eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_url"] = list_null(each_dict.get("url"))
                    eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_domain"] = list_null(each_dict.get("domain"))
                    #---------------------当原始数据中url数据但domain中没有数据时，截取url中的domain添加到模板对应位置中---------------------
                    if eventlog_template_dict["threat_info"]["purpose_endpoint"]["domain_info"]["domain"] == "" and not eventlog_template_dict["threat_info"]["purpose_endpoint"]["url"] == "":
                        domain_op = urlparse(eventlog_template_dict["threat_info"]["purpose_endpoint"]["url"]).netloc
                        eventlog_template_dict["threat_info"]["purpose_endpoint"]["domain_info"]["domain"] = domain_op
                        eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_domain"] = domain_op                
                try:
                    request_handle_list = each_dict.get("http",{}).get("request",{}).get("header",{}).get("other",[{}])
                except:
                    request_handle_list = None

                #for i in each_dict.get("http",{}).get("request",{}).get("header",{}).get("other",[{}]):
                if request_handle_list :
                    for i in request_handle_list:
                        if i:   # 判断字典中是否有值
                            if i["key"] == "Host":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["request"]["header"]["Host"] = i["value"]
                            if i["key"] == "Connection":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["request"]["header"]["Connection"] = i["value"]
                            if i["key"] == "User-Agent":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["request"]["header"]["User-Agent"] = i["value"]
                            if i["key"] == "Accept":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["request"]["header"]["Accept"] = i["value"]
                            if i["key"] == "Referer":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["request"]["header"]["Referer"] = i["value"]
                            if i["key"] == "Accept-Encoding":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["request"]["header"]["Accept-Encoding"] = i["value"]
                            if i["key"] == "Accept-Language":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["request"]["header"]["Accept-Language"] = i["value"]
                            if i["key"] == "Cookie":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["request"]["header"]["Cookie"] = i["value"]                                            
                            if i["key"] == "Contern-Type":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["request"]["header"]["Contern-Type"] = i["value"] 
                            if i["key"] == "Version":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["request"]["header"]["version"] = i["value"]
                        else:
                            break 
            
                # eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["request"]["header"]["Host"] = each_dict.get("http",{}).get("request",{}).get("header",{}).get("host","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["request"]["body"]["bytes"] = str(each_dict.get("http",{}).get("request",{}).get("body",{}).get("bytes",""))
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["request"]["body"]["direction"] = each_dict.get("http",{}).get("request",{}).get("body",{}).get("direction","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["request"]["body"]["file_name"] = each_dict.get("http",{}).get("request",{}).get("body",{}).get("fn","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["request"]["body"]["file_type"] = each_dict.get("http",{}).get("request",{}).get("body",{}).get("ft","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["request"]["body"]["file_hash"]["md5"] = each_dict.get("http",{}).get("request",{}).get("body",{}).get("md5","")

                try:
                    response_handle_list = each_dict.get("http",{}).get("response",{}).get("header",{}).get("other",[{}])
                except:
                    response_handle_list = None
                #for i in each_dict.get("http",{}).get("response",{}).get("header",{}).get("other",[{}]):
                if response_handle_list:
                    for i in response_handle_list:
                        if i:
                            if i["key"] == "Host":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["response"]["header"]["Host"] = i["value"]
                            if i["key"] == "Connection":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["response"]["header"]["Connection"] = i["value"]
                            if i["key"] == "User-Agent":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["response"]["header"]["User-Agent"] = i["value"]
                            if i["key"] == "Accept":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["response"]["header"]["Accept"] = i["value"]
                            if i["key"] == "Referer":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["response"]["header"]["Referer"] = i["value"]
                            if i["key"] == "Accept-Encoding":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["response"]["header"]["Accept-Encoding"] = i["value"]
                            if i["key"] == "Accept-Language":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["response"]["header"]["Accept-Language"] = i["value"]
                            if i["key"] == "Cookie":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["response"]["header"]["Cookie"] = i["value"]                                            
                            if i["key"] == "Contern-Type":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["response"]["header"]["Contern-Type"] = i["value"] 
                            if i["key"] == "Version":
                                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["response"]["header"]["version"] = i["value"] 
                        else:
                            break
            
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["response"]["body"]["bytes"] = str(each_dict.get("http",{}).get("response",{}).get("body",{}).get("bytes",""))
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["response"]["body"]["direction"] = each_dict.get("http",{}).get("response",{}).get("body",{}).get("direction","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["response"]["body"]["file_name"] = each_dict.get("http",{}).get("response",{}).get("body",{}).get("fn","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["response"]["body"]["file_type"] = each_dict.get("http",{}).get("response",{}).get("body",{}).get("ft","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["http_info"]["response"]["body"]["filehash"]["md5"] = each_dict.get("http",{}).get("response",{}).get("body",{}).get("md5","")
            if each_dict.get("msg"):
                ptd_protocol = "smb"
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["smb_info"]["upload"]["bytes"] = str(each_dict.get("msg",{}).get("upload",{}).get("bytes",""))
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["smb_info"]["upload"]["direct"] = each_dict.get("msg",{}).get("upload",{}).get("direction","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["smb_info"]["upload"]["file_name"] = each_dict.get("msg",{}).get("upload",{}).get("fn","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["smb_info"]["upload"]["file_type"] = each_dict.get("msg",{}).get("upload",{}).get("ft","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["smb_info"]["upload"]["file_hash"]["md5"] = each_dict.get("msg",{}).get("upload",{}).get("md5","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["smb_info"]["download"]["bytes"] = str(each_dict.get("msg",{}).get("download",{}).get("bytes",""))
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["smb_info"]["download"]["direct"] = each_dict.get("msg",{}).get("download",{}).get("direction","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["smb_info"]["download"]["file_name"] = each_dict.get("msg",{}).get("download",{}).get("fn","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["smb_info"]["download"]["file_type"] = each_dict.get("msg",{}).get("download",{}).get("ft","")
                eventlog_template_dict["threat_info"]["carrier"]["info"]["data_info"]["smb_info"]["download"]["file_hash"]["md5"] = each_dict.get("msg",{}).get("download",{}).get("md5","")
            eventlog_template_dict["threat_info"]["carrier"]["protocol"] = ptd_protocol    # 判断ptd日志的协议并丰富到eventlog中threat_info.carrier.protocol中
            
            # 丰富目的端点信息
            eventlog_template_dict["threat_info"]["purpose_endpoint"]["ip_info"]["ip"] = each_dict["dst"]["ip"]
            eventlog_template_dict["threat_info"]["purpose_endpoint"]["ip_info"]["port"] = each_dict["dst"]["port"]
            eventlog_template_dict["threat_info"]["purpose_endpoint"]["ip_info"]["mac"] = each_dict["dst"]["mac"]
            eventlog_template_dict["threat_info"]["purpose_endpoint"]["ip_info"]["ipv6"] = each_dict["dst"]["ipv6"]
            eventlog_template_dict["threat_info"]["purpose_endpoint"]["ip_info"]["ip_country"] = each_dict["dst"]["country"]
            eventlog_template_dict["threat_info"]["purpose_endpoint"]["ip_info"]["ip_city"] = each_dict["dst"]["city"]
            eventlog_template_dict["threat_info"]["purpose_endpoint"]["ip_info"]["ip_region"] = each_dict["dst"]["region"]
            # eventlog_template_dict["threat_info"]["purpose_endpoint"]["ip_info"]["ip_cp"] = each_dict["dst"]["unit"]
            eventlog_template_dict["threat_info"]["purpose_endpoint"]["ip_info"]["isp"] = each_dict["dst"]["isp"]
            eventlog_template_dict["threat_info"]["purpose_endpoint"]["ip_info"]["ip_longitude"] = each_dict["dst"].get("location",{}).get("lon","")
            eventlog_template_dict["threat_info"]["purpose_endpoint"]["ip_info"]["ip_longitude"] = each_dict["dst"].get("location",{}).get("lat","")
            # eventlog_template_dict["threat_info"]["purpose_endpoint"]["url"] = list_null(each_dict.get("url"))
            # eventlog_template_dict["threat_info"]["purpose_endpoint"]["domain_info"]["domain"] = list_null(each_dict.get("domain")) 
            # ---------------------当原始数据中url数据但domain中没有数据时，截取url中的domain添加到模板对应位置中---------------------
            # if eventlog_template_dict["threat_info"]["purpose_endpoint"]["domain_info"]["domain"] is "" and not eventlog_template_dict["threat_info"]["purpose_endpoint"]["url"] is "":
            #     eventlog_template_dict["threat_info"]["purpose_endpoint"]["domain_info"]["domain"] = urlparse(eventlog_template_dict["threat_info"]["purpose_endpoint"]["url"]).netloc
            eventlog_template_dict["threat_info"]["purpose_endpoint"]["label"] = each_dict.get("label",[])
            eventlog_template_dict["threat_info"]["purpose_endpoint"]["detect"]["detect_time"] = each_dict["ts"]["start"]
            eventlog_template_dict["threat_info"]["purpose_endpoint"]["detect"]["detect_pro"] = "PTD"
            eventlog_template_dict["threat_info"]["purpose_endpoint"]["detect"]["detect_pro_id"] = each_dict.get("dev")

            # 目的端点的关联资产数据
            # 判断ip是否为内部网络或外部资产ip,若是则调用注册资产的接口，若不是则略过
            if check_private_addr(each_dict["dst"]["ip"]) == 2 or External_Asset(each_dict["dst"]["ip"]) == "waibuzichan":
                cur_d = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # 当ptd中有发包大小为0时的数据时 是全网扫描生成的数据 关联不上资产 所以略过 
                s2c_pkts = each_dict["s2c"]["pkts"]
                s2c_bytes = each_dict["s2c"]["bytes"]
                if s2c_pkts != 0 or s2c_bytes != 0:
                    dst_asset_info_list = get_asset_data(each_dict["dst"]["ip"],each_dict["dst"]["mac"],each_dict.get("dev"),hbase_rowkey,cur_d,conn)
                else:
                    dst_asset_info_list = None
                    logger.warning("s2c_pkts = 0  &&  s2c_bytes = 0,  asset reg pass")
                if isinstance(dst_asset_info_list,list):
                    logger.info("dst_uuid is %s"%(dst_asset_info_list[0]))
                    eventlog_template_dict["threat_info"]["purpose_endpoint"]["asset_detail"]["asset_id"] = dst_asset_info_list[0]
                    eventlog_template_dict["threat_info"]["purpose_endpoint"]["asset_detail"]["os"] = dst_asset_info_list[1]
                    for dst_asset_info_list_ipmac in dst_asset_info_list[2]:
                        dst_ipmac_dict = {}
                        dst_ipmac_dict["ip"] = dst_asset_info_list_ipmac[0]
                        dst_ipmac_dict["mac"] = dst_asset_info_list_ipmac[1]
                        eventlog_template_dict["threat_info"]["purpose_endpoint"]["asset_detail"]["ip_group"].append(dst_ipmac_dict)
                    del eventlog_template_dict["threat_info"]["purpose_endpoint"]["asset_detail"]["ip_group"][0]  
                    eventlog_template_dict["threat_info"]["purpose_endpoint"]["asset_detail"]["unit"] = str_null(dst_asset_info_list[3])
                    eventlog_template_dict["threat_info"]["purpose_endpoint"]["asset_detail"]["network_struct"] = dst_asset_info_list[4]
                    eventlog_template_dict["threat_info"]["purpose_endpoint"]["asset_detail"]["type"] = dst_asset_info_list[5]
                    eventlog_template_dict["threat_info"]["purpose_endpoint"]["asset_detail"]["asset_level"] = dst_asset_info_list[6]
                    eventlog_template_dict["threat_info"]["purpose_endpoint"]["asset_detail"]["people"]["user"] = dst_asset_info_list[7]
                    eventlog_template_dict["threat_info"]["purpose_endpoint"]["asset_detail"]["people"]["manager"] = dst_asset_info_list[8]
                    eventlog_template_dict["threat_info"]["purpose_endpoint"]["asset_detail"]["applications"] = dst_asset_info_list[9]
                    # 端点所属单位的信息 先遵从pg库中查到的 如果没有则遵循ptd的unit字段给出的-----2020-1-14修改------
                    if dst_asset_info_list[3]:
                        eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_ip_cp"] = dst_asset_info_list[3]
                        eventlog_template_dict["threat_info"]["purpose_endpoint"]["ip_info"]["ip_cp"] = dst_asset_info_list[3]
                    else:
                        eventlog_template_dict["Used_by_ES_dst"]["purpose_endpoint_ip_cp"] = each_dict.get("dst",{}).get("unit")
                        eventlog_template_dict["threat_info"]["purpose_endpoint"]["ip_info"]["ip_cp"] = each_dict.get("dst",{}).get("unit")    
            else:
                dst_asset_info_list = None

            # --------------------------丰富事件类别信息--------------------------
            get_threat_type_dict = get_threat_type(each_dict)
            if get_threat_type_dict:
                if "c2_flag" in get_threat_type_dict:
                    eventlog_template_dict["threat_info"]["event_type"]["first_type"] = "网络攻击事件"
                    eventlog_template_dict["threat_info"]["event_type"]["second_type"] = "C2通信"
                    eventlog_template_dict["threat_info"]["event_type"]["third_type"] = str_null(get_threat_type_dict["keyword"])
                    eventlog_template_dict["threat_info"]["sponsor_behavior"]["attack_info"]["first_type"] = "网络攻击事件"
                    eventlog_template_dict["threat_info"]["sponsor_behavior"]["attack_info"]["second_type"] = "C2通信"
                    eventlog_template_dict["threat_info"]["sponsor_behavior"]["attack_info"]["third_type"] = str_null(get_threat_type_dict["keyword"])
                elif "vul_flag" in get_threat_type_dict:
                    eventlog_template_dict["threat_info"]["event_type"]["first_type"] = "网络攻击事件"
                    eventlog_template_dict["threat_info"]["event_type"]["second_type"] = "漏洞攻击事件"
                    eventlog_template_dict["threat_info"]["event_type"]["third_type"] = str_null(get_threat_type_dict["keyword"])
                    eventlog_template_dict["threat_info"]["sponsor_behavior"]["attack_info"]["first_type"] = "网络攻击事件"
                    eventlog_template_dict["threat_info"]["sponsor_behavior"]["attack_info"]["second_type"] = "漏洞攻击事件"
                    eventlog_template_dict["threat_info"]["sponsor_behavior"]["attack_info"]["third_type"] = str_null(get_threat_type_dict["keyword"])              
                else :
                    eventlog_template_dict["threat_info"]["event_type"]["first_type"] = "有害程序"
                    eventlog_template_dict["threat_info"]["event_type"]["third_type"] = str_null(get_threat_type_dict["keyword"])
                    eventlog_template_dict["threat_info"]["sponsor_behavior"]["attack_info"]["first_type"] = "有害程序"
                    eventlog_template_dict["threat_info"]["sponsor_behavior"]["attack_info"]["third_type"] = str_null(get_threat_type_dict["keyword"])                     

            final_data = json.dumps(eventlog_template_dict)
            # print(final_data)
            
            # --------------------------将整理之后的数据存入Kafka--------------------------
            try:
                # add_to_kafka(final_data,'PTD_Nsa_test')
                if isinstance(dst_asset_info_list,bool) or isinstance(src_asset_info_list,bool):
                    final_data = [final_data]   
                    add_to_kafka(final_data,'PTD_asset')
                else:
                    if each_dict.get("is_malicious") or each_dict.get("alert"):
                        # add_to_kafka(final_data,'Standardization_BlackData')
                        BlackData_List.append(final_data)
                        
                    else:
                        # add_to_kafka(final_data,'Standardization_WhiteData')
                        WhiteData_List.append(final_data)

                    
            except IOError as e:
                logger.error('Kafka data processing failed:%s'%(e))
                # print(e)
            load_f.close()

        if len(BlackData_List) >= 1:
            add_to_kafka(BlackData_List,'Standardization_BlackData')
            # logger.info(BlackData_List[0])
            # ----- 20190929 add    storage  redis ---------
            try:
                today = datetime.datetime.now().strftime("%Y-%m-%d")
                origin_key = 'ptd_black_%s' % today
                count = redis_pool.get(origin_key)
                if not count:
                    redis_pool.set(origin_key, len(BlackData_List))
                else:
                    redis_pool.set(origin_key, len(BlackData_List)+int(count))
            except:
                pass
            logger.info('Successful Black_Data processing!!! Time:%s'%(time.ctime(time.time())))
            BlackData_List.clear()
        elif len(WhiteData_List) >= 128:
            add_to_kafka(WhiteData_List,'Standardization_WhiteData')
            # ----- 20200106 add    storage  redis ---------
            try:
                today = datetime.datetime.now().strftime("%Y-%m-%d")
                origin_key = 'ptd_white_%s' % today
                count = redis_pool.get(origin_key)
                if not count:
                    redis_pool.set(origin_key, len(WhiteData_List))
                else:
                    redis_pool.set(origin_key, len(WhiteData_List)+int(count))
            except:
                pass        
            logger.info('Successful White_data processing!!! Time:%s'%(time.ctime(time.time())))
            WhiteData_List.clear()
    conn.close()

def while_true_main():
    while True:
        main() 
        # time.sleep(1)

if __name__ == '__main__':
    # main()


    # # ----------------------单进程循环跑------------------
    # while True:        
    #     main()
    #     time.sleep(1)

    # # ----------------------多进程程------------------
    Process_list = []
    pro_num = int(config_env["pro_num"])
    for i in range(pro_num):
        p = Process(target=while_true_main)  # 多进程
        Process_list.append(p)
    for Process_a in Process_list:
        Process_a.start()
    for Process_p in Process_list:
        Process_p.join()