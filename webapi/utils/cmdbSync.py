#!/usr/bin/env python3.6
# -*- coding:UTF-8 -*-
# __author__: sdc

import os
import sys
import json
import time
import uuid as U
import logging
import requests
import datetime
import configparser

sys.path.append("/opt/DataCollect/webapi")
from log import record
from utils.postgresql import ADataBase

def _init():
    server_dir = "/opt/DataCollect/webapi/"
    global _global_dict
    try:
        global_conf = configparser.ConfigParser()
        global_conf.read("/opt/DataCollect/global.conf")
        conf = configparser.ConfigParser()
        conf.read("/opt/DataCollect/webapi/conf/server.conf")
        pg_conn = json.loads(global_conf.get("PG", "pg_conn"))
        cmdb_ip = conf.get("CMDB", "ip")
        cmdb_port = conf.get("CMDB", "port")
        logger = record(filename='cmdb_assetSync.log', level=logging.WARNING)
        _global_dict = {
                        "server_dir": server_dir,
                        "pg_conn": pg_conn,
                        "cmdb_ip": cmdb_ip,
                        "cmdb_port": cmdb_port,
                        "logger": logger
        }
    except Exception as e:
        print(str(e))
        sys.exit()
    return _global_dict

def getModelData(cmdb_ip, cmdb_port, logger):
    modelIdList = []
    try:
        url = "http://%s:%s/cmdbservice/modelPage/getConfigCategoryTreeBySyc" % (cmdb_ip, cmdb_port)
        response = requests.get(url, timeout=3, verify=False)
        message = response.text
        modelIdData = json.loads((message.strip()))
    except Exception as e:
        logger.error(str(e))
        modelIdData = []
    for eachmodelId in modelIdData:
        try:
            id = eachmodelId["id"]
            parent_id = eachmodelId["parent_id"]
        except Exception as e:
            logger.error(str(e))
            continue
        if not id in modelIdList and not parent_id in modelIdList:
            modelIdList.append(id)
    return modelIdList

def getAssetData(cmdb_ip, cmdb_port, modelId, logger):
    try:
        url = "http://%s:%s/cmdbservice/categoryManage/selectModel?modelType=1&modelId=%s" % (cmdb_ip, cmdb_port, modelId)
        response = requests.post(url, timeout=3, verify=False)
        message = response.text
        assetData = json.loads(message.strip())["pageData"]
    except Exception as e:
        logger.error(str(e))
        assetData = []
    return assetData

def syncAsset(assetData, pg_conn, logger):
    dctl = ADataBase()
    dctl.connect_db(pg_conn)
    sql = "select asset_id from h_asset_ip_info where ip_addr=%s"
    assetTable = "h_hardware_asset_info"
    assetIpTable = "h_asset_ip_info"
    assetMapTabel = "h_asset_source_record"
    with_ip_counter = 0
    without_ip_counter = 0
    insert_counter = 0
    update_counter = 0
    for eachAsset in assetData:
        try:
            ip = eachAsset["cmdb_ip"]
            if not ip:
                without_ip_counter += 1
                continue
            with_ip_counter += 1
            asset_name = eachAsset["cmdb_name"]
            mperson = eachAsset["cmdb_zrr_id_value"]
            #position = eachAsset["mp_2"]
            #cmdb_state = eachAsset["cmdb_state_value"]
        except Exception as e:
            logger.error(str(e))
            continue
        create_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        args = (ip, )
        flag, asset_id = dctl.get_table_flag(sql, params=args)
        if flag  == True:
            # 资产管理中已经存在cmdb中的资产ip信息， 则更新资产管理中的信息
            if asset_id:
                uuid = asset_id[0][0]
                wherelist = {"asset_id": uuid}
                flushdata = {
                             "asset_name": asset_name,
                             "asset_perfect_source": "3",
                             "update_time": create_time,
                             "update_person": mperson

                }
                flag = dctl.update_db(assetTable, flushdata, wherelist)
                if flag:
                    update_counter += 1
                    dctl.commit()
            # 资产管理中不存在cmdb中的资产ip信息， 则在资产管理中插入资产信息
            else:
                asset_id = str(U.uuid5(U.NAMESPACE_DNS, str(U.uuid1()))).replace("-", "").upper()
                map_asset_id = str(U.uuid5(U.NAMESPACE_DNS, str(U.uuid1()))).replace("-", "").upper()
                dataDict = {
                             "asset_id": asset_id,
                             "asset_name": asset_name,
                             "node_id": "1",
                             "asset_level": 3,
                             "register_state": "0",
                             "delete_state": "1",
                             "create_time": create_time,
                             "create_person": mperson,
                             "source": "3",
                }
                flag, message = dctl.insert_db(assetTable, dataDict.keys(), dataDict)
                if not flag:
                    logger.error(message)
                    return

                dataDict = {
                            "asset_id": asset_id,
                            "ip_addr": ip,
                            "group_id": "1"
                }
                flag, message = dctl.insert_db(assetIpTable, dataDict.keys(), dataDict)
                if not flag:
                    logger.error(message)
                    return
                
                dataDict = {
                            "first_asset_id": asset_id,
                            "map_asset_id": map_asset_id,
                            "asset_source": "3"
                }
                flag, message = dctl.insert_db(assetMapTabel, dataDict.keys(), dataDict)
                if not flag:
                    logger.error(message)
                    return

                insert_counter += 1
                dctl.commit()

        else:
            continue
    dctl.close()
    logger.warning("without_ip_asset: %s" % without_ip_counter)
    logger.warning("with_ip_asset: %s" % with_ip_counter)
    logger.warning("insert_asset: %s" % insert_counter)
    logger.warning("update_asset: %s" % update_counter)

def main(config_env):
    cmdb_ip = config_env["cmdb_ip"]
    cmdb_port = config_env["cmdb_port"]
    pg_conn = config_env["pg_conn"]
    logger = config_env["logger"]
    logger.warning("sync start ...")
    modelList = getModelData(cmdb_ip, cmdb_port, logger)
    if modelList:
        for eachmodelId in modelList:
            assetData = getAssetData(cmdb_ip, cmdb_port, eachmodelId, logger)
            if assetData:
                logger.warning("asset found: %s" % len(assetData))
                syncAsset(assetData, pg_conn, logger)
    logger.warning("sync finish ... ,  300 seconds sleep")

if __name__ == "__main__":
    config_env = _init()
    while 1:
        main(config_env)
        time.sleep(300)
