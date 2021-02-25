#!/usr/bin/env python3.6
# -*- coding:UTF-8 -*-
# __author__: sdc

import json as djson
from sanic import Blueprint
from auth import authorized
from sanic.response import json

from utils.postgresql import ADataBase

import Config
config_env = Config.get_value()

asset = Blueprint("asset")

logger = config_env["logger"]

@asset.listener('before_server_start')
async def setup_connection(app, loop):
    pass

@asset.listener('after_server_stop')
async def close_connection(app, loop):
    pass



'''
    cmdb资产数据更新-新建、修改

    接口名称：/api/cmdb/asset/update

    功能        ：cmdb资产数据更新,包含新增或修改（资产信息、硬件信息、软件信息）。

'''
@asset.route('/api/cmdb/asset/update', methods=['POST'])
@authorized()
async def asset_update(request):
    try:
        body = djson.loads(request.body.decode())
        data = body["data"] if "data" in body else []
    except Exception as e:
        logger.error(str(e))
        response = {"message": "E2"}
        return json(response)

    response = {"message": "S0", "data": data}
    return json(response)


'''
    cmdb资产数据更新-删除、恢复

    接口名称：/api/cmdb/asset/del

    功能        ：cmdb资产数据更新,将资产移入、移出回收站操作。

'''
@asset.route('/api/cmdb/asset/del', methods=['POST'])
@authorized()
async def asset_recover(request):
    try:
        body = djson.loads(request.body.decode())
        ips = body["ips"] if "ips" in body else []
        manul = body["manul"] if "manul" in body else ""
    except Exception as e:
        logger.error(str(e))
        response = {"message": "E2"}
        return json(response)

    response = {"message": "S0", "data": ips}
    return json(response)


'''
    cmdb系统资产数据同步

    接口名称：/api/cmdb/asset/sync

    功能        ：将资产管理中资产数据提供给cmdb系统使用

'''
@asset.route('/api/cmdb/asset/sync', methods=['POST'])
@authorized()
async def asset_recover(request):
    pg_conf = config_env["pg_conn"]
    dctl = ADataBase()
    dctl.connect_db(pg_conf)

    # 查询资产信息
    sql = '''select asset_id, node_name, type_name, asset_name, asset_level, model, os_ver, ainfo.remarks, manage_person, manage_contact, position, application, 
                application_manage, hardware_manage, create_person, create_contact, delete_state
            from h_hardware_asset_info ainfo left join h_asset_type atype on ainfo.type_id = atype.type_id
			    left join sys_node_info sinfo on ainfo.node_id = sinfo.node_id and sinfo.state = '1'
    
    '''
    assetData = dctl.get_table_flag(sql)
    if not assetData[0]:
        response = {"message": "E0"}
        return json(response)

    # 查询IP信息
    ip_sql = '''select asset_id, ip_addr, mac_addr from h_asset_ip_info'''

    # 查询软件信息
    soft_sql = '''select asset_id, software_type_name, software_name, software_version, publisher, to_char(install_time, 'yyyy-mm-dd hh24:mi:ss' ) as install_time, install_addr, state  
                    from h_software_info sinfo left join k_software_type stype on sinfo.software_type_id = stype.software_type_id'''

    # 查询服务信息
    serv_sql = ''''''

    # 查询硬件信息
    hard_sql = '''select asset_id, hardware_type_name, brand, manufacturer, model, capacity, serial_number, frequency, thread_num, core_num, 
                        interface_type, version, firmware_version, bios_version, bios_time, ip_addr, mac_addr, net_card_type, is_connecte, 
                        driver_version, gcard_memory, drive_letter, cd_type 
                  from h_hardware_detail hdetail left join k_hardware_type htype on hdetail.hardware_type_id = htype.hardware_type_id  
                    and htype.state='1' '''
    assetData = assetData[-1]

    ipData = dctl.get_table_flag(ip_sql)
    if not ipData[0]:
        response = {"message": "E0"}
        return json(response)
    ipData = ipData[-1]

    softData = dctl.get_table_flag(soft_sql)
    if not softData[0]:
        response = {"message": "E0"}
        return json(response)
    softData = softData[-1]
    
    hardData = dctl.get_table_flag(hard_sql)
    if not hardData[0]:
        response = {"message": "E0"}
        return json(response)
    hardData = hardData[-1]

    data = {
            "asset_info": assetData,
            "ip_info": ipData,
            "software_info": softData,
            "hardware_info": hardData
    }
    response = {"message": "S0", "data": data}
    return json(response)
