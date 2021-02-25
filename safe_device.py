'''
@Author: sdc
@Date: 2020-01-08 09:47:23
LastEditTime: 2020-08-12 15:13:25
LastEditors: Please set LastEditors
@Description:  生成数据运营报告(excel)脚本
@FilePath: /opt/data_service/makeExcel.py
'''


#!/usr/bin/env python3.6
# -*- coding:utf-8 -*-
# __author__: sdc

import re
import json
import redis
import datetime
import xlsxwriter

with open("device.json", "r") as ff:
    deviceMap = json.load(ff)

class BuildExcel(object):

    def __init__(self, ip, port, db, passwd):
        self.redis_ip = ip
        self.redis_port = port
        self.redis_db = db
        self.redis_passwd = passwd

    def connect_redis(self):
        # 开启redis连接池
        redis_pool = redis.ConnectionPool(
                                            host=self.redis_ip,
                                            port=self.redis_port,
                                            db=self.redis_db,
                                            password=self.redis_passwd,
                                            decode_responses=True
        )
        redis_conn = redis.Redis(connection_pool=redis_pool)
        return redis_conn

    def get_data(self, redis_conn, day):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
        # 获取今日安全设备在线总数
        online = redis_conn.get("online")
        if online:
            online = json.loads(online)
        else:
            online = {}

        # 获取iep最近上传数据时间
        iepdata_time = redis_conn.get("iep")
        if iepdata_time:
            iepdata_time = json.loads(iepdata_time)
        else:
            iepdata_time = {}
        
        # 获取ptd最近上传数据时间
        ptddata_time = redis_conn.get("ptd")
        if ptddata_time:
            ptddata_time = json.loads(ptddata_time)
        else:
            ptddata_time = {}

        # 获取iep最近心跳时间
        iepheart_time = redis_conn.zrange("iep_heart", 0, -1, withscores=True)
        if not iepheart_time:
            iepheart_time = []
        
        # 获取ptd最近心跳时间
        ptdheart_time = redis_conn.zrange("ptd_heart", 0, -1, withscores=True)
        if not ptdheart_time:
            ptdheart_time = []
        
        # 获取iep上传数据条数
        ieplog_count = redis_conn.get("iep_%s" % timestamp)
        if ieplog_count:
            ieplog_count = json.loads(ieplog_count)
        else:
            ieplog_count = {}

        # 获取ptd上传数据条数
        ptdlog_count = redis_conn.get("ptd_%s" % timestamp)
        if ptdlog_count:
            ptdlog_count = json.loads(ptdlog_count)
        else:
            ptdlog_count = {}

        # 获取今日安全设备上报数据量
        ptd_origin_count = redis_conn.get("ptd_origin_%s" % timestamp)
        if not ptd_origin_count:
            ptd_origin_count = 0
        else:
            ptd_origin_count = int(ptd_origin_count)
        ptd_black_count = redis_conn.get("ptd_black_%s" % timestamp)
        if not ptd_black_count:
            ptd_black_count = 0
        else:
            ptd_black_count = int(ptd_black_count)
        iep_origin_count = redis_conn.get("iep_origin_%s" % timestamp)
        if not iep_origin_count:
            iep_origin_count = 0
        else:
            iep_origin_count = int(iep_origin_count)
        iep_black_count = redis_conn.get("iep_black_%s" % timestamp)
        if not iep_black_count:
            iep_black_count = 0
        else:
            iep_black_count = int(iep_black_count)
        star_origin_count = redis_conn.get("star_origin_%s" % timestamp)
        if not star_origin_count:
            star_origin_count = 0
        else:
            star_origin_count = int(star_origin_count)
        star_black_count = redis_conn.get("star_black_%s" % timestamp)
        if not star_black_count:
            star_black_count = 0
        else:
            star_black_count = int(star_black_count)
        hillstone_origin_count = redis_conn.get("hillstone_origin_%s" % timestamp)
        if not hillstone_origin_count:
            hillstone_origin_count = 0
        else:
            hillstone_origin_count = int(hillstone_origin_count)
        hillstone_black_count = redis_conn.get("hillstone_black_%s" % timestamp)
        if not hillstone_black_count:
            hillstone_black_count = 0
        else:
            hillstone_black_count = int(hillstone_black_count)
        gap_origin_count = redis_conn.get("gap_origin_%s" % timestamp)
        if not gap_origin_count:
            gap_origin_count = 0
        else:
            gap_origin_count = int(gap_origin_count)
        gap_black_count = redis_conn.get("gap_black_%s" % timestamp)
        if not gap_black_count:
            gap_black_count = 0
        else:
            gap_black_count = int(gap_black_count)

        data = {
                "online": online,
                "iepdata_time": iepdata_time,
                "ptddata_time": ptddata_time,
                "iepheart_time": iepheart_time,
                "ptdheart_time": ptdheart_time,
                "ieplog_count": ieplog_count,
                "ptdlog_count": ptdlog_count,
                "ptd_origin_count": ptd_origin_count,
                "ptd_black_count": ptd_black_count,
                "iep_origin_count": iep_origin_count,
                "iep_black_count": iep_black_count,
                "star_origin_count": star_origin_count,
                "star_black_count": star_black_count,
                "hillstone_origin_count": hillstone_origin_count,
                "hillstone_black_count": hillstone_black_count,
                "gap_origin_count": gap_origin_count,
                "gap_black_count": gap_black_count
        }

        return data
        
    def write2excel(self, day, data):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        online = data["online"]
        iepdata_time = data["iepdata_time"]
        ptddata_time = data["ptddata_time"]
        iepheart_time = data["iepheart_time"]
        ptdheart_time = data["ptdheart_time"]
        ieplog_count = data["ieplog_count"]
        ptdlog_count = data["ptdlog_count"]
        ptd_origin_count = data["ptd_origin_count"]
        ptd_black_count = data["ptd_black_count"]
        iep_origin_count = data["iep_origin_count"]
        iep_black_count = data["iep_black_count"]
        star_origin_count = data["star_origin_count"]
        star_black_count = data["star_black_count"]
        hillstone_origin_count = data["hillstone_origin_count"]
        hillstone_black_count = data["hillstone_black_count"]
        gap_origin_count = data["gap_origin_count"]
        gap_black_count = data["gap_black_count"]

        sub = "安全设备上报数据日报"
        filename = u'{0}-{1}.xlsx'.format(sub, day)
        workbook = xlsxwriter.Workbook("/opt/DataCollect/excel/%s" % filename)
        titleConfig = {
            'border': 1,  # 单元格边框宽度
            'font_name': '楷体',
            'font_size':14,  #字体大小
            'align': 'left',
            'valign': 'vcenter',  # 字体对齐方式
            'fg_color': '#F4B084',  # 单元格背景颜色
        }
        titleConfig_nobg = {
            'border': 1,  # 单元格边框宽度
            'font_name': '楷体',
            'font_size':14,  #字体大小
            'align': 'center',
            'valign': 'vcenter',  # 字体对齐方式
        }
        contentConfig = {
            'border': 1,  # 单元格边框宽度
            'font_name': '楷体',
            'font_size':14,                #字体大小
            'align': 'center',
            'valign': 'vcenter',  # 字体对齐方式
            'text_wrap': True,  # 是否自动换行
            #'num_format':'yyyy-mm-dd'
        }
        #'font_color':'#FEFEFE',        #字体颜色
        numberConfig = {
            'border': 1,  # 单元格边框宽度
            'font_name': '楷体',
            'font_size':14,                #字体大小
            'align': 'center',
            'valign': 'vcenter',  # 字体对齐方式
            'text_wrap': True,  # 是否自动换行
            'num_format': '#,##0'
        }

        # 今日数据日报
        titlebold = workbook.add_format(titleConfig)
        contentbold = workbook.add_format(contentConfig)
        numberbold = workbook.add_format(numberConfig)

        worksheet = workbook.add_worksheet('今日数据日报')
        worksheet.set_column('A:I', 30)
        for eachrow in range(0, 2000):
            worksheet.set_row(eachrow, 30)
        worksheet.write_row('A1', ["厂商", "日志数量", "日志总数量", "黑日志数"], contentbold)
        worksheet.write_row('A2', ["启明", star_origin_count], numberbold)
        worksheet.merge_range('C2:C3', star_origin_count+hillstone_origin_count, numberbold)
        worksheet.write_column('D2', [star_black_count], numberbold)
        worksheet.write_row('A3', ["山石", hillstone_origin_count], numberbold)
        worksheet.write_column('D3', [hillstone_black_count], numberbold)
        worksheet.write_row('A4', ["IEP", iep_origin_count, iep_origin_count, iep_black_count], numberbold)
        worksheet.write_row('A5', ["PTD", ptd_origin_count, ptd_origin_count, ptd_black_count], numberbold)
        worksheet.write_row('A6', ["网闸", gap_origin_count, gap_origin_count, gap_black_count], numberbold)

        worksheet = workbook.add_worksheet('今日在线安全设备') 
        worksheet.set_column('A:I', 30)
        for eachrow in range(0, 2000):
            worksheet.set_row(eachrow, 30)
        flag = 0
        for device, info in online.items():
            if device == 'zhenguan':
                continue
            name = [device] 
            for eachinfo in info:
                chinesename = ''
                for key, value in deviceMap.items():
                    tag = re.match(r'.*%s' % key, eachinfo)
                    if tag:
                        chinesename = value
                        break
                if not chinesename:
                    chinesename = eachinfo
                name.append(chinesename)
            worksheet.write_column('%s1' % chr(ord('A') + flag), name, contentbold)
            flag += 1

        # iep最近上传数据时间
        worksheet = workbook.add_worksheet('iep最近上传数据时间') 
        worksheet.set_column('A:I', 30)
        for eachrow in range(0, 2000):
            worksheet.set_row(eachrow, 30)
        worksheet.write_row('A1', ["IEP", "最近上报数据时间"], contentbold)
        flag = 2
        for device, info in iepdata_time.items():
            chinesename = ''
            for key, value in deviceMap.items():
                tag = re.match(r'.*%s' % key, device)
                if tag:
                    chinesename = value
                    break
            if not chinesename:
                chinesename = device
            worksheet.write_row('A%s' % flag, [chinesename, info["last_send_time"]], contentbold)
            flag += 1

        # ptd最近上传数据时间
        worksheet = workbook.add_worksheet('ptd最近上传数据时间') 
        worksheet.set_column('A:I', 30)
        for eachrow in range(0, 2000):
            worksheet.set_row(eachrow, 30)
        worksheet.write_row('A1', ["PTD", "最近上报数据时间"], contentbold)
        flag = 2
        for device, info in ptddata_time.items():
            chinesename = ''
            for key, value in deviceMap.items():
                tag = re.match(r'.*%s' % key, device)
                if tag:
                    chinesename = value
                    break
            if not chinesename:
                chinesename = device
            worksheet.write_row('A%s' % flag, [chinesename, info["last_send_time"]], contentbold)
            flag += 1

        # iep最近发送心跳时间
        worksheet = workbook.add_worksheet('iep最近发送心跳时间') 
        worksheet.set_column('A:I', 30)
        for eachrow in range(0, 2000):
            worksheet.set_row(eachrow, 30)
        worksheet.write_row('A1', ["IEP", "最近发送心跳时间"], contentbold)
        flag = 2
        for info in iepheart_time:
            device = info[0]
            stime = info[-1]
            stime = datetime.datetime.fromtimestamp(stime).strftime("%Y-%m-%d %H:%M:%S")
            chinesename = ''
            for key, value in deviceMap.items():
                tag = re.match(r'.*%s' % key, device)
                if tag:
                    chinesename = value
                    break
            if not chinesename:
                chinesename = device
            worksheet.write_row('A%s' % flag, [chinesename, stime], contentbold)
            flag += 1

        # ptd最近发送心跳时间
        worksheet = workbook.add_worksheet('ptd最近发送心跳时间') 
        worksheet.set_column('A:I', 30)
        for eachrow in range(0, 2000):
            worksheet.set_row(eachrow, 30)
        worksheet.write_row('A1', ["PTD", "最近发送心跳时间"], contentbold)
        flag = 2
        for info in ptdheart_time:
            device = info[0]
            stime = info[-1]
            stime = datetime.datetime.fromtimestamp(stime).strftime("%Y-%m-%d %H:%M:%S")
            chinesename = ''
            for key, value in deviceMap.items():
                tag = re.match(r'.*%s' % key, device)
                if tag:
                    chinesename = value
                    break
            if not chinesename:
                chinesename = device
            worksheet.write_row('A%s' % flag, [chinesename, stime], contentbold)
            flag += 1

        # iep上传日志条数
        worksheet = workbook.add_worksheet('iep上传日志条数') 
        worksheet.set_column('A:I', 30)
        for eachrow in range(0, 2000):
            worksheet.set_row(eachrow, 30)
        worksheet.write_row('A1', ["IEP", "上传总日志条数", "上传黑日志条数"], contentbold)
        flag = 2
        for device, count in ieplog_count.items():
            chinesename = ''
            for key, value in deviceMap.items():
                tag = re.match(r'.*%s' % key, device)
                if tag:
                    chinesename = value
                    break
            if not chinesename:
                chinesename = device
            worksheet.write_row('A%s' % flag, [chinesename, count["origin"], count["black"]], numberbold)
            flag += 1

        # ptd上传日志条数
        worksheet = workbook.add_worksheet('ptd上传日志条数') 
        worksheet.set_column('A:I', 30)
        for eachrow in range(0, 2000):
            worksheet.set_row(eachrow, 30)
        worksheet.write_row('A1', ["PTD", "上传总日志条数", "上传黑日志条数"], contentbold)
        flag = 2 
        for device, count in ptdlog_count.items():
            chinesename = ''
            for key, value in deviceMap.items():
                tag = re.match(r'.*%s' % key, device)
                if tag:
                    chinesename = value
                    break
            if not chinesename:
                chinesename = device
            worksheet.write_row('A%s' % flag, [chinesename, count["origin"], count["black"]], numberbold)
            flag += 1

        workbook.close()

def makeExcel(today):
    ip = "10.255.175.96"
    port = 6379
    db = 2
    passwd = "antiy?pmc"
    build = BuildExcel(ip, port, db, passwd)
    redis_conn = build.connect_redis()
    data = build.get_data(redis_conn, today)
    build.write2excel(today, data)

if __name__ == "__main__":
    today = datetime.datetime.now().strftime("%Y%m%d")
    makeExcel(today)

