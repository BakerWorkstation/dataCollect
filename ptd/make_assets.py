# -*- coding:UTF-8 -*-
# __author__: zhangyue
import json
from kafka_operating import add_to_kafka,read_from_kafka
import psycopg2
import psycopg2.extras
import requests
import hashlib
import time
import datetime
import redis
from struct import unpack
from socket import AF_INET,inet_pton,inet_aton


# 从Config.py中加载进程环境变量
import Config; Config._init()
config_env = Config.get_value()

kafka_topic = config_env["asset_topic"]
group_id = config_env["group_id"]
reset = config_env["reset"]
redis_pool = config_env["redis_pool"]
logger = config_env["logger_assets"]

# postgres获取句柄
pg_conf = json.loads(config_env["pg_conn"])
# conn = psycopg2.connect(database=pg_conf["database"], user=pg_conf["user"], password=pg_conf["password"], host=pg_conf["host"], port=pg_conf["port"])
# cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# 生成资产API
asset_api = config_env["asset_reg"]

# 匹配本应该是string的字段是否是null值 如果是null则输出空字符串，如果是正常字符则返回该值
def str_null(obj):
    #if isinstance(obj,basestring):
    if isinstance(obj,(str, bytes)):
        return obj
    else:
        return ""

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

# 判断是否是公司的外部资产
def External_Asset(str_ip):   
    int_ip = unpack('!I', inet_aton(str_ip))[0]
    # 1.189.209.202 - 1.189.209.254 公司外部资产的ip范围
    if 29217226 <= int_ip and int_ip <= 29217278:
        return "waibuzichan"

def get_asset_data(ip_info,mac_info,dev_info,cur,conn): 
    asset_info = []
    # 只根据ip确定资产
    cur.execute("SELECT DISTINCT asset_id,group_id FROM h_asset_ip_info WHERE ip_addr=%s",(ip_info,))
    uuid_git_list = cur.fetchone()    
    if uuid_git_list:       
        uuid = uuid_git_list[0]
        gid = uuid_git_list[1]
        cur.execute("SELECT ip_addr,mac_addr FROM h_asset_ip_info WHERE asset_id = %s",(uuid,))
        # ip_mac_list = cur.fetchone()
        # ip_list = ip_mac_list[0]
        # mac = ip_mac_list[1]
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
        # asset_info.append(ip_list)
        # asset_info.append(mac)
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
        # 判断ip是否为内部网络,若是则调用注册资产的接口，若不是则略过
        if check_private_addr(ip_info) == 2 or External_Asset(ip_info) == "waibuzichan":
            #先查一下设备id对应的组织机构标识 若查不到则写为“1”
            cur.execute("SELECT node_id FROM h_device_info WHERE device_id=%s",(dev_info,))
            nodeID_list = cur.fetchone()
            if nodeID_list:
                nodeID = nodeID_list[0]
            else:
                nodeID = "1"

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
                "gid": asset_gid,                                 # 资产组标识   "1":顶级域 "2":内网域 "3":外网域
                "ip": [ip_info],                               # 资产ip列表
                "host_ip": None,                            # 如果资产分类为虚拟资产则有宿主机ip
                "mac": [mac_info],                             # mac地址列表
                "child_group": ["1"],                       # 资产组子域地址列表
                "special": "",                                 # 资产分类   "1": 虚拟设备  "2": 实体设备
                "oid":nodeID,                                     # 组织机构标识!!!
                "position": "",                                # 部署位置
                "cperson_name": dev_info,                      # 创建人(此处为PTD设备ID)
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
                return False
            else:
                if response.status_code == 200:
                    uuid_getfrom_webapi = json.loads(response.text).get("asset_id")
                    return [uuid_getfrom_webapi,"",[[propertykeys["ip"][0],propertykeys["mac"][0]]],None,[],"",propertykeys["level"],propertykeys["cperson_name"],"",[]]
                else:
                    return False               
        else:
            return      

def main():
    conn = psycopg2.connect(database=pg_conf["database"], user=pg_conf["user"], password=pg_conf["password"], host=pg_conf["host"], port=pg_conf["port"])
    
    for i in read_from_kafka(kafka_topic,group_id,reset):
        source_data = json.loads(i)
        # source_data = json.loads(source_data)
        # 源端点的关联资产数据
        s_ip = source_data["threat_info"]["source_endpoint"]["ip_info"]["ip"]
        s_mac = source_data["threat_info"]["source_endpoint"]["ip_info"]["mac"]
        s_dev = source_data["threat_info"]["source_endpoint"]["detect"]["detect_pro_id"]
        cur_s = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        src_asset_info_list = get_asset_data(s_ip,s_mac,s_dev,cur_s,conn)
        if isinstance(src_asset_info_list,list):
            source_data["threat_info"]["source_endpoint"]["asset_detail"]["asset_id"] = src_asset_info_list[0]
            source_data["threat_info"]["source_endpoint"]["asset_detail"]["os"] = src_asset_info_list[1]
            for src_asset_info_list_ipmac in src_asset_info_list[2]:
                src_ipmac_dict = {}
                src_ipmac_dict["ip"] = src_asset_info_list_ipmac[0]
                src_ipmac_dict["mac"] = src_asset_info_list_ipmac[1]
                source_data["threat_info"]["source_endpoint"]["asset_detail"]["ip_group"].append(src_ipmac_dict)
            del source_data["threat_info"]["source_endpoint"]["asset_detail"]["ip_group"][0]                    
            source_data["threat_info"]["source_endpoint"]["asset_detail"]["unit"] = str_null(src_asset_info_list[3])
            source_data["threat_info"]["source_endpoint"]["asset_detail"]["network_struct"] = src_asset_info_list[4]
            source_data["threat_info"]["source_endpoint"]["asset_detail"]["type"] = src_asset_info_list[5]
            source_data["threat_info"]["source_endpoint"]["asset_detail"]["asset_level"] = src_asset_info_list[6]
            source_data["threat_info"]["source_endpoint"]["asset_detail"]["people"]["user"] = src_asset_info_list[7]
            source_data["threat_info"]["source_endpoint"]["asset_detail"]["people"]["manager"] = src_asset_info_list[8]
            source_data["threat_info"]["source_endpoint"]["asset_detail"]["applications"] = src_asset_info_list[9]

        # 目的端点的关联资产数据
        d_ip = source_data["threat_info"]["purpose_endpoint"]["ip_info"]["ip"]
        d_mac = source_data["threat_info"]["purpose_endpoint"]["ip_info"]["mac"]
        d_dev = source_data["threat_info"]["purpose_endpoint"]["detect"]["detect_pro_id"]
        cur_d = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        dst_asset_info_list = get_asset_data(d_ip,d_mac,d_dev,cur_d,conn)
        if isinstance(dst_asset_info_list,list):
            source_data["threat_info"]["purpose_endpoint"]["asset_detail"]["asset_id"] = dst_asset_info_list[0]
            source_data["threat_info"]["purpose_endpoint"]["asset_detail"]["os"] = dst_asset_info_list[1]
            for dst_asset_info_list_ipmac in dst_asset_info_list[2]:
                dst_ipmac_dict = {}
                dst_ipmac_dict["ip"] = dst_asset_info_list_ipmac[0]
                dst_ipmac_dict["mac"] = dst_asset_info_list_ipmac[1]
                source_data["threat_info"]["purpose_endpoint"]["asset_detail"]["ip_group"].append(dst_ipmac_dict)
            del source_data["threat_info"]["purpose_endpoint"]["asset_detail"]["ip_group"][0]  
            source_data["threat_info"]["purpose_endpoint"]["asset_detail"]["unit"] = str_null(dst_asset_info_list[3])
            source_data["threat_info"]["purpose_endpoint"]["asset_detail"]["network_struct"] = dst_asset_info_list[4]
            source_data["threat_info"]["purpose_endpoint"]["asset_detail"]["type"] = dst_asset_info_list[5]
            source_data["threat_info"]["purpose_endpoint"]["asset_detail"]["asset_level"] = dst_asset_info_list[6]
            source_data["threat_info"]["purpose_endpoint"]["asset_detail"]["people"]["user"] = dst_asset_info_list[7]
            source_data["threat_info"]["purpose_endpoint"]["asset_detail"]["people"]["manager"] = dst_asset_info_list[8]
            source_data["threat_info"]["purpose_endpoint"]["asset_detail"]["applications"] = dst_asset_info_list[9]

        final_data = json.dumps(source_data)
        final_data = [final_data]

        # --------------------------将整理之后的数据存入Kafka--------------------------
        try:
            if isinstance(dst_asset_info_list,bool) or isinstance(src_asset_info_list,bool):  
                add_to_kafka(final_data,'PTD_asset')                
            else:
                if source_data.get("is_malicious") or source_data.get("alert"):
                    add_to_kafka(final_data,'Standardization_BlackData')
                    # ----- 20190929 add    storage  redis ---------
                    try:
                        today = datetime.datetime.now().strftime("%Y-%m-%d")
                        origin_key = 'ptd_black_%s' % today
                        count = redis_pool.get(origin_key)
                        if not count:
                            redis_pool.set(origin_key, 1)
                        else:
                            redis_pool.set(origin_key, 1+int(count))
                    except:
                        pass
                    logger.info('Successful Black_data processing from asset!!! Time:%s'%(time.ctime(time.time())))
                else:
                    add_to_kafka(final_data,'Standardization_WhiteData')
                    # ----- 20200106 add    storage  redis ---------
                    try:
                        today = datetime.datetime.now().strftime("%Y-%m-%d")
                        origin_key = 'ptd_white_%s' % today
                        count = redis_pool.get(origin_key)
                        if not count:
                            redis_pool.set(origin_key, 1)
                        else:
                            redis_pool.set(origin_key, 1+int(count))
                    except:
                        pass        
                    logger.info('Successful White_data processing from asset!!! Time:%s'%(time.ctime(time.time())))
                logger.info('Kafka data processing success')           
        except IOError as e:
            logger.error('Kafka data processing failed:%s'%(e))
            # print(e)
            # print('Kafka data processing failed')   
    conn.close()

if __name__ == '__main__':
    while True:
        main()
        time.sleep(1)
