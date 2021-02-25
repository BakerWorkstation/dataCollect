#!/usr/bin/env python3.6
# -*- coding:utf-8 -*-
# __author__: sdc

import re
import sys
import redis
import datetime

def count(timestamp):


    redis_ip = "10.255.175.96"
    redis_port = "6379"
    redis_db = 2
    redis_passwd = "antiy?pmc"

    ptd_origin = "ptd_origin_%s" % timestamp
    ptd_black =  "ptd_black_%s" % timestamp

    iep_origin = "iep_origin_%s" % timestamp
    iep_black =  "iep_black_%s" % timestamp

    #star_origin = "star_origin_%s" % timestamp
    #star_black =  "star_black_%s" % timestamp

    #hillstone_origin = "hillstone_origin_%s" % timestamp
    #hillstone_black =  "hillstone_black_%s" % timestamp

    #gap_origin = "gap_origin_%s" % timestamp
    #gap_black =  "gap_black_%s" % timestamp

    zhenguan_origin = "zhenguan_origin_%s" % timestamp
    zhenguan_black =  "zhenguan_black_%s" % timestamp

    pool = redis.ConnectionPool(
        host=redis_ip,
        port=redis_port,
        db=redis_db,
        password=redis_passwd,
        decode_responses=True
    )
    redis_pool = redis.Redis(connection_pool=pool)
    pipe = redis_pool.pipeline(transaction=True)
    ptd_origin_count = redis_pool.get(ptd_origin)
    ptd_black_count = redis_pool.get(ptd_black)
    iep_origin_count = redis_pool.get(iep_origin)
    iep_black_count = redis_pool.get(iep_black)
    #star_origin_count = redis_pool.get(star_origin)
    #star_black_count = redis_pool.get(star_black)
    #hillstone_origin_count = redis_pool.get(hillstone_origin)
    #hillstone_black_count = redis_pool.get(hillstone_black)
    #gap_origin_count = redis_pool.get(gap_origin)
    #gap_black_count = redis_pool.get(gap_black)
    zhenguan_origin_count = redis_pool.get(zhenguan_origin)
    zhenguan_black_count = redis_pool.get(zhenguan_black)

    pipe.execute()
    print("%s 数据日报" % timestamp)
    print("-" * 20)
    print("ptd原始日志数量: %s" % ptd_origin_count)
    print("-" * 20)
    print("ptd黑日志数量: %s" % ptd_black_count)
    print("-" * 20)
    print("iep原始日志数量: %s" % iep_origin_count)
    print("-" * 20)
    print("iep黑日志数量: %s" % iep_black_count)
    print("-" * 20)
    #print("网闸原始日志数量: %s" % gap_origin_count)
    #print("-" * 20)
    #print("网闸黑日志数量: %s" % gap_black_count)
    #print("-" * 20)
    #print("启明原始日志数量: %s" % star_origin_count)
    #print("-" * 20)
    #print("启明黑日志数量: %s" % star_black_count)
    #print("-" * 20)
    #print("山石原始日志数量: %s" % hillstone_origin_count)
    #print("-" * 20)
    #print("山石黑日志数量: %s" % hillstone_black_count)
    #print("-" * 20)
    print("镇关原始日志数量: %s" % zhenguan_origin_count)
    print("-" * 20)
    print("镇关黑日志数量: %s" % zhenguan_black_count)
    print("-" * 20)

if __name__ == "__main__":
    timestamp = input("输入日期 (2019-01-01): ")
    if not re.match( r'\d{4}-\d{2}-\d{2}', timestamp.strip()):
        print("日期格式有误")
        sys.exit(0)
    count(timestamp)
