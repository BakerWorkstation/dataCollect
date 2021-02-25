#!/usr/bin/env python3.6
# -*- coding:UTF-8 -*-
# __author__: sdc

import json as djson
from sanic import Blueprint
from sanic.response import json
from werkzeug.security import generate_password_hash

import Config
config_env = Config.get_value()

login = Blueprint("login")

logger = config_env["logger"]
cmdb_u = config_env["cmdb_u"]
cmdb_p = config_env["cmdb_p"]
redis_pool = config_env["redis_pool"]
redis_expire = config_env["redis_expire"]


@login.listener('before_server_start')
async def setup_connection(app, loop):
    pass

@login.listener('after_server_stop')
async def close_connection(app, loop):
    pass

@login.route('/login', methods=['POST'])
async def create_token(request):
    try:
        body = djson.loads(request.body.decode())
        username = body["username"] if "username" in body else ""
        password = body["password"] if "password" in body else ""
    except Exception as e:
        logger.error(str(e))
        response = {"message": "E2"}
        return json(response)
    pipe = redis_pool.pipeline(transaction=True)

    # 判断用户名、密码
    if username != cmdb_u or password != cmdb_p:
        response = {"message": "E4"}
        return json(response)
    else:
        try:
            token = redis_pool.get(username)
        except redis.exceptions.ConnectionError as e:
            logger.error(str(e))
            response = {"message": "E0"}
            return json(response)
        # 判断用户是否已经登录
        if not token:
            token = generate_password_hash(password)
            try:
                redis_pool.set(username, token)
                redis_pool.expire(username, int(redis_expire))  # 过期时间
            except redis.exceptions.ConnectionError as e:
                logger.error(str(e))
                response = {"message": "E0"}
                return json(response)
    pipe.execute()

    response = {"message": "S0", "data": token}
    return json(response)
