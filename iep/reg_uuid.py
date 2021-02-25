import sys
import json
import requests

# 从Config.py中加载进程环境变量
import Config; Config._init()
config_env = Config.get_value()

sys.path.append("/opt/DataCollect")
from append_asset import append_asset

class Reg_uuid(object):
    def __init__(self):
        pass
    def getuuid(self,asset_api,clientuuid,ip,serveruuid):
        propertykeys = {
                        "asset_id": clientuuid,              # 资产标识
                        "node_id": '1',                    # 组织机构标识!!!
                        "type_id": '1',                    # 资产类型(不能"")
                        "asset_level": '3',                # 资产等级
                        "asset_label": '',                 # 资产标签
                        "model": "",                       # 资产型号
                        "host_ip": "",                     # 如果资产分类为虚拟资产则有宿主机ip
                        "position": "",                    # 部署位置
                        "asset_classify": "",              # 资产分类   "1": 虚拟设备  "2": 实体设备
                        "source": "0",                     # 资产来源 "0": iep "1": 人工录入 "2":ptd "3":cmdb "4":防火墙
                        "create_person": serveruuid,       # 创建人（此处为iep服务端设备id）
                        "use_person": "",                  # 使用人
                        "use_contact": "",                 # 使用人联系方式
                        "ip_addr": ip,                     # ip
                        "mac_addr": "",                    # mac
                        "group_id": "1"                    # 资产组标识  "1":内网资产 "2":外网资产
        }
        try:
            result = append_asset(propertykeys, json.loads(config_env["pg_conn"]))
        except Exception as e:
            result = {'message': 'fail', 'error': str(e)}

        return result

