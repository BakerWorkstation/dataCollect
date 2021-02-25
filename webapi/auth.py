#!/usr/bin/env python3.6
# -*- coding:UTF-8 -*-
# __author__: sdc

import json as djson
from log import record
from functools import wraps
from sanic.response import json

import Config
config_env = Config.get_value()

logger = config_env["logger"]
redis_pool = config_env["redis_pool"]

def check_request_for_authorization_status(request):
    try:
        body = djson.loads(request.body)
        token = body["token"]
        username = body["username"]
    except Exception as e:
        logger.error(str(e))
        return False
    pipe = redis_pool.pipeline(transaction=True)
    try:
        redis_token = redis_pool.get(username)
    except redis.exceptions.ConnectionError as e:
        logger.error(str(e))
        redis_token = None
    except:
        redis_token = None
    pipe.execute()
    if redis_token == token:
        return True

    return False

def authorized():
    def decorator(function):
        @wraps(function)
        async def decorated_function(request, *args, **kwargs):
            # run some method that checks the request
            # for the client's authorization status
            is_authorized = check_request_for_authorization_status(request)

            if is_authorized:
                # the user is authorized.
                # run the handler method and return the response
                response = await function(request, *args, **kwargs)
                return response
            else:
                # the user is not authorized.
                response = {"message": "E3"}
                return json(response, 200)
        return decorated_function
    return decorator