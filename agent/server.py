#!/usr/bin/env python3.6
# -*- coding:utf-8 -*-
# __author__: sdc

import json
import time
import os.path
import requests
requests.packages.urllib3.disable_warnings()
# 从Config.py中加载进程环境变量
import Config; Config._init()
config_env = Config.get_value()

class file_read:
    def __init__(self):
        self._logfilename = config_env["logfile"]
        self.logger = config_env["logger"]
        self.username = config_env["cmdb_u"]
        self.password = config_env["cmdb_p"]
        self.api_ip = config_env["api_ip"]
        self.api_port = config_env["api_port"]
        self.get_old_data()

    def get_old_data(self):
        f = open(self._logfilename, 'r')
        data = f.readlines()
        f.close()
        response = self.auth()
        if response["message"] == "S0":
            token = response["data"]
            self.send(token, data)

    def auth(self):
        url = "https://%s:%s/login" % (self.api_ip, self.api_port)
        body = {
                "username": self.username,
                "password": self.password
        }
        connect = requests.post(url=url, data=json.dumps(body), verify=False)
        response = connect.text
        return json.loads(response)

    def send(self, token, data):
        url = "https://%s:%s/api/data/collect" % (self.api_ip, self.api_port)
        body = {
                "username": self.username,
                "token": token,
                "data": data
        }
        connect = requests.post(url=url, data=json.dumps(body), verify=False)
        response = connect.text
        return json.loads(response)

    def get_time(self, _logfilename):
        if os.path.exists(_logfilename):
            return time.ctime(os.path.getctime(_logfilename))
        else:
            self.logger.error(str(self._logfilename) + 'is not exist,after 1s system will check again')
            time.sleep(1)
            self.get_time(_logfilename)

    def file_readlines(self, line):
        self.logger.info(line)

    def file_readline(self):
        if os.path.exists(self._logfilename):
            f = open(self._logfilename, 'r')
        else:
            self.logger.error(str(self._logfilename) + 'is not exist,after 1s system will check again')
            time.sleep(1)
            self.file_readline()

        f.seek(0, 2)
        # before_ctime=time.ctime(os.path.getctime(self._logfilename))
        before_ctime = self.get_time(self._logfilename)
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                # after_ctime=time.ctime(os.path.getctime(self._logfilename))
                after_ctime = self.get_time(self._logfilename)
                after_offset = f.tell()
                if offset == after_offset and before_ctime != after_ctime and os.path.exists(self._logfilename):
                    f.close()
                    f = open(self._logfilename,'r')
                    line = f.readline()
                    self.file_readlines(line)
                    before_ctime = self.get_time(self._logfilename)
                    continue
                time.sleep(0.05)
            else:
                self.file_readlines(line)
                response = self.auth()
                if response["message"] == "S0":
                    token = response["data"]
                    self.send(token, [line])
                before_ctime = self.get_time(self._logfilename)
        f.close()

if __name__ == '__main__':
    ff = file_read()
    ff.file_readline()