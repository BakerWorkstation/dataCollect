'''
@Author: sdc
@Date: 2020-01-18 13:37:40
@LastEditTime : 2020-01-19 16:00:10
@LastEditors  : Please set LastEditors
@Description: In User Settings Edit
@FilePath: /opt/DataCollect/webapi/App/data_service.py
'''
#!/usr/bin/env python3.6
# -*- coding:UTF-8 -*-
# __author__: sdc

import os
import sys
import random
import datetime
import json as djson
from sanic import Blueprint, response
from auth import authorized
from sanic.response import json, html, text
from jinja2 import Environment, PackageLoader, select_autoescape, Template

sys.path.append("/opt/data_service")
from makeExcel import makeExcel

import Config
config_env = Config.get_value()
logger = config_env["logger"]
redis_pool = config_env["redis_pool"]
print(redis_pool)

# 初始化blueprint并定义静态文件夹路径
ds = Blueprint('data_service')
ds.static('/static/business/css/bootstrap-combined.min.css', './static/business/css/bootstrap-combined.min.css')
ds.static('/static/business/css/bootstrap.min.css', './static/business/css/bootstrap.min.css')
ds.static('/static/business/js/jquery-3.2.0.js', './static/business/js/jquery-3.2.0.js')
ds.static('/static/business/js/bootstrap.min.js', './static/business/js/bootstrap.min.js')
ds.static('/static/business/js/echarts-all.js', './static/business/js/echarts-all.js')
ds.static('/static/business/img/plus.gif', './static/business/img/plus.gif')
ds.static('/static/business/img/minus.gif', './static/business/img/minus.gif')

 
# jinjia2 config
env = Environment(
    loader=PackageLoader('views.data_service', '../template/business'),
    autoescape=select_autoescape(['html', 'xml', 'tpl']))
 
def template(tpl, **kwargs):
    template = env.get_template(tpl)
    return html(template.render(kwargs))

@ds.listener('before_server_start')
async def setup_connection(app, loop):
    pass

@ds.listener('after_server_stop')
async def close_connection(app, loop):
    pass


'''
    数据运营报告

    接口名称：/api/data/detail

    功能        ：查看运营数据结果

'''
@ds.route('/api/data/detail', methods=['GET'])
async def data_detail(request):
    # try:
    #     body = djson.loads(request.body.decode())
    #     data = body["data"] if "data" in body else []
    # except Exception as e:
    #     logger.error(str(e))
    #     response = {"message": "E2"}
    #     return json(response)

    #response = {"message": "S0"}
    #return json(response)
    #return render_template('test.html')
    return template('detail.html', title='index')


'''
    数据

    接口名称：/api/data/get

    功能        ：提供数据

'''
@ds.route('/api/data/get', methods=['POST'])
async def data_detail(request):
    report_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #day = datetime.datetime.now().strftime("%Y%m%d")
    body = djson.loads(request.body.decode())
    date = body["date"] if "date" in body else ""
    day = date.split("T")[0].replace("-", "")
    #logger.warning(day)
    #day = "20200118"
    # try:
    #     body = djson.loads(request.body.decode())
    #     data = body["data"] if "data" in body else []
    # except Exception as e:
    #     logger.error(str(e))
    #     response = {"message": "E2"}
    #     return json(response)

    # 获取ptd原始日志相关数据
    ptd_info = redis_pool.get("ptd_%s" % day)
    if ptd_info:
        ptd_info = djson.loads(ptd_info)
    else:
        ptd_info = {}

    # 获取iep原始日志相关数据
    iep_info = redis_pool.get("iep_%s" % day)
    if iep_info:
        iep_info = djson.loads(iep_info)
    else:
        iep_info = {}

    # 获取标准化数量
    stand_black_count = redis_pool.get("stand_black_%s" % day)
    if stand_black_count:
        stand_black_count = djson.loads(stand_black_count)
    else:
        stand_black_count = {}
    stand_gray_count = redis_pool.get("stand_white_%s" % day)
    if stand_gray_count:
        stand_gray_count = djson.loads(stand_gray_count)
    else:
        stand_gray_count = {}
    
    # 获取防火墙原始日志相关数据
    zhenguan_info = redis_pool.get("zhenguan_%s" % day)
    if zhenguan_info:
        zhenguan_info = djson.loads(zhenguan_info)
    else:
        zhenguan_info = {}

    # 丰富PTD相关数据
    ptd_data = []
    ptd_device = ["BCNHYW2", "CJXJWW2", "2102311QGK10HA000177", "BC3CYW2", "BBMHYW2", "1NKVRT2", "C0GDWW2", "CPBLWW2"]
    ptd_device.insert(0, "设备编号")
    ptd_data.append(ptd_device)
    ptd_data.append(["部署位置", "墙外-电信镜像(外网)", "墙外-联通镜像(外网)", "墙内-办公网镜像(内网)", "机房d排(内网)", "机房c排(内网)", "机房a排(内网)", "机房e排(内网)", "wifi(内网)-暂未接入"])
    ptd_data.append(["IP地址", "10.255.190.2", "10.255.190.3", "10.255.49.100", "10.255.190.5", "10.255.190.6", "10.255.190.7", "10.255.190.8", "10.255.190.1"])
    ptd_data.append(["是否全要素", "是", "是", "是", "是", "是", "是", "是", ""])
    heart_data = ["最近活跃时间"]
    origin_total = ["原始总日志数量"]
    origin_black = ["原始黑日志数量"]
    origin_gray = ["原始灰日志数量"]
    asset_count = ["捕获资产数量"]
    stand_black = ["标准化黑日志数量"]
    stand_gray = ["标准化灰日志数量"]

    ptd_online = []
    for device in ptd_device[1:]:
        hearttime = redis_pool.get("ptd_%s" % device)
        if hearttime:
            ptd_online.append(device)
        heart_data.append(hearttime)
        countinfo = ptd_info.get(device)
        if not countinfo:
            origin_total.append("")
            origin_black.append("")
            origin_gray.append("")
            asset_count.append("")
        else:
            origin_total.append(countinfo.get("total"))
            origin_black.append(countinfo.get("black"))
            origin_gray.append(countinfo.get("gray"))
            asset_count.append(countinfo.get("asset_count"))
        stand_black.append(stand_black_count.get(device))
        stand_gray.append(stand_gray_count.get(device))

    ptd_data.append(heart_data)
    ptd_data.append(origin_total)
    ptd_data.append(origin_black)
    ptd_data.append(origin_gray)
    ptd_data.append(asset_count)
    ptd_data.append(stand_black)
    ptd_data.append(stand_gray)
    
    # 丰富IEP相关数据
        # 获取ptd设备最后活跃时间
    iep_data = []
    iep_device = ["设备编号", "fdd27420-1c6a-42a0-aaa2-cc8e6519b681", "51dd13ad8deeba44208f34e648d63918"]
    iep_ip = ["IP地址", "10.255.49.17", "10.255.52.122"]
    iep_data.append(iep_device)
    iep_data.append(["部署位置", "C11-37-38(内网)", "C11-40-41(内网)"])
    iep_data.append(iep_ip)
    heart_data = ["最近活跃时间"]
    origin_total = ["原始总日志数量"]
    origin_black = ["原始黑日志数量"]
    origin_gray = ["原始灰日志数量"]
    asset_count = ["捕获资产数量"]
    stand_black = ["标准化黑日志数量"]
    stand_gray = ["标准化灰日志数量"]
    iep_online = []
    for device in iep_ip[1:]:
        hearttime = redis_pool.get("iep_%s" % device)
        if hearttime:
            iep_online.append(device)
        heart_data.append(hearttime)
        countinfo = iep_info.get(device)
        if not countinfo:
            origin_total.append("")
            origin_black.append("")
            origin_gray.append("")
            asset_count.append("")
        else:
            origin_total.append(countinfo.get("total"))
            origin_black.append(countinfo.get("black"))
            origin_gray.append(countinfo.get("gray"))
            asset_count.append(countinfo.get("asset_count"))
        stand_black.append(stand_black_count.get(device))
        stand_gray.append(stand_gray_count.get(device))

    iep_data.append(heart_data)
    iep_data.append(origin_total)
    iep_data.append(origin_black)
    iep_data.append(origin_gray)
    iep_data.append(asset_count)
    iep_data.append(stand_black)
    iep_data.append(stand_gray)

    # 丰富防火墙相关数据
    firewall_data = []
    firewall_device = ["设备编号", "镇关-办公网防火墙", "华为-联通出口防火墙", "华为-出口入侵检测"]
    firewall_ip = ["IP地址", "10.255.192.242", "10.0.0.17", "10.255.192.249"]
    firewall_data.append(firewall_device)
    firewall_data.append(["部署位置", "办公网透明过滤防火墙", "联通光纤出口NAT和透明过滤防火墙", "联通、电信、ADSL出口入侵检测"])
    firewall_data.append(["IP地址", "10.255.192.242", "10.0.0.17", "10.255.192.249"])
    heart_data = ["最近活跃时间"]
    origin_total = ["原始总日志数量"]
    app_policy = ["APP_POLICY"]
    adsl = ["ADSL"]
    firewall_online = []
    for device in firewall_ip[1:]:
        hearttime = redis_pool.get("firewall_%s" % device)
        if hearttime:
            firewall_online.append(device)
        heart_data.append(hearttime)
        countinfo = zhenguan_info.get(device)
        if not countinfo:
            origin_total.append("")
            app_policy.append("")
            adsl.append("")
        else:
            origin_total.append(countinfo.get("total"))
            app_policy.append(countinfo.get("APP_POLICY"))
            adsl.append(countinfo.get("ADSLs"))

    firewall_data.append(heart_data)
    firewall_data.append(origin_total)


    response = {
                "message": "S0", 
                "report_time": report_time, 
                "ptd": ptd_data, 
                "ptd_online": ptd_online,
                "iep": iep_data,
                "iep_online": iep_online,
                "firewall": firewall_data,
                "firewall_online": firewall_online
    }
    return json(response)



'''
    数据

    接口名称：/api/data/download/excel

    功能        ：提供数据

'''
@ds.route('/api/data/download/excel')
async def data_detail(request):
    date = request.args.get('date')
    day = date.split("T")[0].replace("-", "")
    logger.warning(day)
    filename = "/opt/data_service/excel/试运营数据报告-%s.xlsx" % day
    if os.path.exists(filename):
        pass
    else:
        makeExcel(day)

    return await response.file(filename, headers={'Content-Disposition': 'attachment;filename=试运营数据报告-%s.xlsx' % day}, mime_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

