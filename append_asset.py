# -*- coding: UTF-8 -*-
import datetime
import json
import re
import uuid
from IPy import IP
import psycopg2
from psycopg2.extras import RealDictCursor

'''
description:   将IP地址转成数值
param {type} 
return {type}  返回整形数值
'''
def convert_ip_to_number(ip_str):
    ret = 0
    ip_str=ip_str.strip()
    parts = ip_str.split('.')
    if len(parts) == 4:
        ret = int(parts[0]) * 256 * 256 * 256 + int(parts[1]) * 256 * 256 + int(parts[2]) * 256  + int(parts[3])
    return ret


'''
description:   将IP地址和掩码 转成数值区间
param {type} 
return {type}    返回列表：包含IP开始和IP结束两个元素
'''
def calIpvalue(ipaddr, netmask):
    iprange = IP(ipaddr).make_net(netmask).strNormal()
    ip = IP(iprange)
    data = []
    for x in (ip[0], ip[-1]):
        data.append((x.int()))
    return data


def organization(data, ip_value):
    inset = set()
    outset = set()
    for eachdata in data:
        flag = False
        node_id = eachdata["node_id"]
        networktype = eachdata["networktype"]
        data_type = eachdata["data_type"]
        ip = eachdata["ip"]
        end_ip = eachdata["end_ip"]
        mask = eachdata["mask"]
        if networktype == "1":
            start_ip = convert_ip_to_number(ip)
            end_ip = convert_ip_to_number(end_ip)
            if ip_value >= start_ip and ip_value <= end_ip:
                flag = True
        elif networktype == "2":
            ip_range = calIpvalue(ip, mask)
            start_ip = ip_range[0]
            end_ip = ip_range[-1]
            if ip_value >= start_ip and ip_value <= end_ip:
                flag = True
        elif networktype == "3":
            start_ip = convert_ip_to_number(ip)
            if ip_value == start_ip:
                flag = True
        else:
            pass
        if flag:
            if data_type == '0':
                inset.add(node_id)
            else:
                outset.add(node_id)

    return inset - outset

def __get_new_uuid():
    """获取新的uuid"""
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(uuid.uuid1()))).replace("-", "").upper()


def __insert_asset_source_record(first_asset_id, asset_source, map_asset_id, r_device, cursor):
    """插入资产多源历史记录表"""
    sql = """
        insert into h_asset_source_record(
            first_asset_id,
            asset_source,
            map_asset_id,
            r_device,
            r_time
        ) values(
            %(first_asset_id)s,
            %(asset_source)s,
            %(map_asset_id)s,
            %(r_device)s,
            %(r_time)s
        )
    """
    sql_dict = {
        "first_asset_id": first_asset_id,
        "asset_source": asset_source,
        "map_asset_id": map_asset_id,
        "r_device": r_device,
        "r_time": datetime.datetime.now()
    }
    cursor.execute(sql, sql_dict)


def __update_label(asset_label, cursor):
    """处理标签"""
    asset_label = asset_label.split(',')
    lable_id_list = []
    select_sql = "select label_id from k_asset_label where label_name=%(label_name)s"
    insert_sql = """
        insert into k_asset_label(
            label_id,
            label_name,
            r_person,
            r_time
        ) values(
            %(label_id)s,
            %(label_name)s,
            %(r_person)s,
            %(r_time)s
        )
    """
    for label in asset_label:
        cursor.execute(select_sql, {"label_name": label})
        label_id = cursor.fetchall()
        if label_id:
            lable_id_list.append(label_id[0]['label_id'])
        else:
            sql_dict = {
                "label_id": __get_new_uuid(),
                "label_name": label,
                "r_person": "asset acquisition",
                "r_time": datetime.datetime.now()
            }
            cursor.execute(insert_sql, sql_dict)
            lable_id_list.append(sql_dict['label_id'])
    return ','.join(lable_id_list)


def append_asset(asset_info, conn):
    """添加资产"""
    conn_flag = False
    if type(conn) == dict:
        conn_flag = True
        conn = psycopg2.connect(**conn)
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    asset_id = asset_info["asset_id"]

    # 查询资产是否已存在
    sql = """
        select 
            a.asset_id 
        from 
            h_hardware_asset_info as a 
            left join h_asset_ip_info as b on a.asset_id = b.asset_id 
        where 
            a.delete_state = '1'
        and b.ip_addr = %(ip_addr)s
    """
    cursor.execute(sql, asset_info)
    old_id = cursor.fetchall()
    if old_id:
        # 资产已存在，记录后返回
        old_id = old_id[0]['asset_id']

        # 检查资产多源历史记录表记录
        sql = """
            select 
                r_id
            from 
                h_asset_source_record 
            where 
                first_asset_id = %(old_id)s 
            and asset_source = %(source)s
            and map_asset_id = %(asset_id)s
        """
        sql_dict = {
            "old_id": old_id,
            "source": asset_info['source'],
            "asset_id": asset_info['asset_id']
        }
        cursor.execute(sql, sql_dict)
        if not cursor.fetchall():
            pass
            #__insert_asset_source_record(old_id, asset_info['source'], asset_id, asset_info['create_person'], cursor)

        # 返回资产id
        res = {
            "message": "ok",
            "asset_id": old_id
        }
    else:
        # 资产不存在，插入新资产数据
        new_asset_id = __get_new_uuid()
        res = {
            "message": "ok",
            "asset_id": new_asset_id
        }
        # 获取设备ID对应的组织结构
        sql = "select distinct node_id from h_device_info where device_id=%s"
        cursor.execute(sql, (asset_info["create_person"], ))
        data = cursor.fetchone()
        if data:
            node_id = data["node_id"]
        # 通过资产IP与组织结构IP进行判断
        sql = "select node_id, data_type, networktype, ip, end_ip, mask from h_node_network"
        cursor.execute(sql)
        data = cursor.fetchall()
        if data:
            organset = organization(data, calIpvalue(asset_info["ip_addr"]))
            if not len(organset) == 1:
                node_id = 1
            else:
                node_id = list(organset)[0]
            asset_info['node_id'] = node_id
        # 插入资产多源历史记录表
        #__insert_asset_source_record(new_asset_id, asset_info['source'], asset_id, asset_info['create_person'], cursor)

        # 处理标签
        asset_info['asset_label'] = __update_label(asset_info['asset_label'], cursor)

        # 插入资产信息
        sql = """
            insert into h_hardware_asset_info (
                asset_id, 
                node_id, 
                type_id, 
                asset_level, 
                asset_label, 
                model, 
                host_ip, 
                position, 
                asset_classify, 
                source, 
                create_person, 
                use_person, 
                use_contact, 
                register_state, 
                delete_state, 
                create_time, 
                fall_state
            ) values(
                %(asset_id)s, 
                %(node_id)s, 
                %(type_id)s, 
                %(asset_level)s, 
                %(asset_label)s,  
                %(model)s,  
                %(host_ip)s,
                %(position)s,
                %(asset_classify)s,
                %(source)s,  
                %(create_person)s,  
                %(use_person)s,  
                %(use_contact)s,  
                %(register_state)s, 
                %(delete_state)s, 
                %(create_time)s, 
                %(fall_state)s
            ) 
        """
        asset_info['asset_id'] = new_asset_id
        asset_info['create_time'] = datetime.datetime.now()
        asset_info['register_state'] = '0'
        asset_info['delete_state'] = '1'
        asset_info['fall_state'] = '0'
        if asset_info['host_ip'] and not re.match(r"^\s*\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\s*$", asset_info['host_ip']):
            raise Exception('host_ip: ' + asset_info['host_ip'] + '格式错误')
        cursor.execute(sql, asset_info)

        # 插入ip信息
        sql = """
            insert into h_asset_ip_info (
                asset_id,
                ip_addr,
                mac_addr,
                group_id
            ) values (
                %(asset_id)s,
                %(ip_addr)s,
                %(mac_addr)s,
                %(group_id)s
            )
        """
        if not re.match(r"^\s*\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\s*$", asset_info['ip_addr']):
            raise Exception('ip: ' + asset_info['ip_addr'] + '格式错误')
        if asset_info['mac_addr'] and \
                not re.match(r"^\s*([0-9a-fA-F]{2,2}-){5,5}[0-9a-fA-F]{2,2}\s*$", asset_info['mac_addr']):
            raise Exception('mac: ' + asset_info['mac_addr'] + '格式错误')
        cursor.execute(sql, asset_info)

    conn.commit()
    cursor.close()
    if conn_flag:
        conn.close()
    return res

if __name__ == '__main__':
    # conn为数据库配置时，append_asset会自己创建连接、提交和关闭
    # conn直接为数据库连接，append_asset会执行提交，但不会关闭传入的连接
    asset = {'asset_id': '0A09D771DE1A5CF89D3A840C0AA8D6B1', 'node_id': '1', 'type_id': '1', 'asset_level': '3', 'asset_label': '', 'model': '', 'host_ip': '', 'position': '', 'asset_classify': '', 'source': '4', 'create_person': '10.2.253.100', 'use_person': '', 'use_contact': '', 'ip_addr': '10.130.6.3', 'mac_addr': '', 'group_id': '1'}
    conn = {'host': '10.255.175.111', 'port': 5432, 'database': 'acd_data_hn', 'user': 'postgres', 'password': 'postgresql_superuser'}

    a = append_asset(asset, conn)
    print(a)
