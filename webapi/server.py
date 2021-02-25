#!/usr/bin/env python3.6
# -*- coding:UTF-8 -*-
# __author__: sdc

import os
import ssl
import sys
import uvloop
import asyncio
import configparser
#import aiotask_context as context
from sanic import Sanic
from sanic.response import json
from sanic_compress import Compress

# 从Config.py中加载进程环境变量
import Config; Config._init()
config_env = Config.get_value()

from auth import authorized
# 加载App下的工程文件
from App.asset import asset
from App.login import login
from App.collect import collect
from views.data_service import ds

app = Sanic(__name__)
Compress(app)

@app.listener('before_server_start')
async def setup_connection(app, loop):
    pass

@app.listener('after_server_stop')
async def close_connection(app, loop):
    pass

# @app.route("/",  methods=['GET', 'POST'])
# #@authorized()
# async def testRoute(request):
#     return json({"message": "S0","data": "test route" })

# 注册蓝图
app.blueprint(login)
app.blueprint(asset)
app.blueprint(collect)
app.blueprint(ds)

if __name__ == "__main__":
    server_dir = config_env["server_dir"]
    web_ip = config_env["web_ip"]
    web_port = config_env["web_port"]
    web_process = config_env["web_process"]
    context = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(
                            os.path.join(server_dir, "ca/server.crt"),
                            keyfile=os.path.join(server_dir, "ca/server.key")
    )
    app.run(
            host=web_ip,
            port=web_port,
            workers=web_process,
            ssl=context,
            debug=False
    )
    # asyncio.set_event_loop(uvloop.new_event_loop())
    # server = app.create_server(
    #                             host=web_ip,
    #                             port=web_port,
    #                             ssl=context,
    #                             return_asyncio_server=True,
    #                             debug=True
    # )
    # loop = asyncio.get_event_loop()
    # loop.set_task_factory(context.task_factory)
    # task = asyncio.ensure_future(server)
    # try:
    #     loop.run_forever()
    # except:
    #     loop.stop()
