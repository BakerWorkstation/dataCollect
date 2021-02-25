#!/usr/bin/env python
# -*- coding:utf-8 -*-

import re
import json
import time
import os
import traceback
import thread
import socket
import ConfigParser

"""
功能：完成推送日志到syslog服务器，兼容了日志转存的情况
"""
# 配置文件地址
CONF_PATH = "conf/config.conf"

# Facility constants
LOG_KERN = 0
LOG_USER = 1
LOG_MAIL = 2
LOG_DAEMON = 3
LOG_AUTH = 4
LOG_SYSLOG = 5
LOG_LPR = 6
LOG_NEWS = 7
LOG_UUCP = 8
LOG_CRON = 9
LOG_AUTHPRIV = 10
LOG_LOGTP = 11
LOG_NTP = 12
LOG_AUDIT = 13
LOG_ALERT = 14
LOG_CLOCK = 15
LOG_LOCAL0 = 16
LOG_LOCAL1 = 17
LOG_LOCAL2 = 18
LOG_LOCAL3 = 19
LOG_LOCAL4 = 20
LOG_LOCAL5 = 21
LOG_LOCAL6 = 22
LOG_LOCAL7 = 23

# Severity ants Names reasonably shortened
LOG_EMERG = 0
LOG_ALERT = 1
LOG_CRIT = 2
LOG_ERR = 3
LOG_WARNING = 4
LOG_NOTICE = 5
LOG_INFO = 6
LOG_DEBUG = 7


class FileManage(object):
    """
    文件控制类
    """
    def __init__(self):
        # 当前行位置
        self.currline = 0
        # 文件夹路径
        self.logpath = "/var/log/app/"
        # 读取增量
        self.plusline = 5
        # 最新转存文件名
        self.rotate_name = self.get_last_file()
        # 日志文件名
        self.logname = "app.log"

    def get_log(self):
        """
        获取日志
        @return list<str> 日志列表
        """
        list_log = list()
        if self.is_log_rotate():
            ########## 测试功能使用，实际应该删除
            self.currline = 0
            # 先获取转存日志中未读取部分
            list_log.extend(self.get_data_by_line(self.logpath + self.rotate_name))
            self.currline = 0
            self.rotate_name = self.get_last_file()
        list_log.extend(self.get_data_by_line(self.logpath + self.logname))
        return list_log

    def get_last_file(self):
        """
        获取最新的转存文件名
        @return <str> 文件名
        """
        # 获取路径下所有文件名
        files = os.listdir(self.logpath)
        repo = r"(?P<name>\bapp.log)-(?P<date>\d{8}\b)"
        file_list = list()
        for each in files:
            # 匹配结果
            re_result = re.match(repo, each)
            if str(re_result) == "None":
                continue
            file_list.append(re_result.groupdict())
        file_list.sort(key=lambda dict: dict['date'], reverse=True)
        # 返回最新文件名
        return file_list[0]['name'] + '-' + file_list[0]['date']

    def get_data_by_line(self, filepath):
        """
        获取需要推送的日志
        @param <int> 开始文件行数
        @param <int> 截止文件行数
        @return list<str> 获取到的日志列表
        """
        log_list = list()
        # 文件不存在返回空列表
        if os.path.exists(filepath):
            with open(filepath, "r") as fobj:
                log_data = fobj.read().splitlines(False)
        else:
            return list()
        log_list = log_data[self.currline:len(log_data)]
        self.currline = len(log_data)
        return log_list

    def is_log_rotate(self):
        """
        判断日志是否被转存
        @return True：已经被转存
                False：未被转存
        """
        return False if self.rotate_name == self.get_last_file() else True


class Transform(object):
    """
    日志内容处理基类
    """
    def __init__(self):
        # 输入字符串
        self.input_str = ""

    def transform_str(self, input_str):
        pass


class JsonFormat(Transform):
    """
    将日志字符串处理成json字符串
    """
    def __init__(self):
        """记录参数"""
        self.facility = LOG_USER
        self.severity = LOG_INFO

    def transform_str(self, input_str):
        """将日志字符串解析成json格式"""
        # 字典key为time,host,owner,pid,level,other,msg
        # ()? 有日志存在Oct__9 10:10:10情况
        repo = r"(?P<time>\w+ ( )?\d+ \d{2}:\d{2}:\d{2}) (?P<host>\w+) (?P<owner>\w+):( +)?(?P<pid>(\[\d+\])?)(?P<level>(\[\w+\])?) ?(?P<other>(\[\w+\])?) ?(?P<msg>(.*))"
        re_result = re.match(repo, input_str)

        if re_result:
            re_dict = re_result.groupdict()

        # 没有匹配到，返回空字符串
        else:
            return str()

        self.facility, self.severity = self.confirm_leave(re_dict['owner'], re_dict['level'])
        # 计算日志等级pri
        pri = self.facility + self.severity
        # syslog协议日志头e.g:Oct 9 22:33:20 localhost
        header = re_dict['time'] + " " + re_dict['host']
        # 进程号
        pid = re_dict['pid']
        # 日志模块
        owner = re_dict['owner']
        # 日志消息
        msg = re_dict['msg']

        return json.dumps(
            dict(PRI=str(pri), HEADER=header, PID=pid, OWNER=owner, MSG=msg),
            sort_keys=True)

    def confirm_leave(self, owner, level):
        """
        根据日志中的owner,level两个信息，确定facility,severity
        """
        # 给定默认值
        facility = LOG_USER
        severity = LOG_INFO
        # as日志级别对应syslog协议级别
        level_info = dict(INFO=LOG_INFO, WARN=LOG_WARNING,
                          ERROR=LOG_ERR, DEBUG=LOG_DEBUG)

        if level in level_info.keys():
            severity = level_info[level]

        # 判定级别
        if owner in ['ecms_troubleshoot']:
            severity = LOG_EMERG

        return facility, severity


class UDPClient(object):
    """UDP 传输"""
    def __init__(self, ip, port):
        """UDP 消息"""
        self._addr = socket.getaddrinfo(ip, port)[0][4]
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def __del__(self):
        """析构函数"""
        self._sock.close()


class SyslogManage(UDPClient):
    """ """
    def __init__(self, ip="127.0.0.1", port=514):
        """
        给定默认日志等级
        """
        super(SyslogManage, self).__init__(ip=ip, port=port)
        self.facility = LOG_USER
        self.severity = LOG_INFO
        self.message = ""

    def push_log_to_server(self):
        """推送日志"""
        try:
            # 单次日志大小超过 128 * 1024 会被不记录该条日志，因此将日志切分记录
            step = 1024
            for i in xrange(0, len(self.message), step):
                self.sendlog(self.message)
        except BaseException:
            traceback.print_exc()
        # print self.message

    def sendlog(self, msg):
        data = "<%d>%s" % (self.severity + (self.facility << 3), msg)
        print(data)
        self._sock.sendto(data.encode(), self._addr)


def logthread(cycle, number):
    """
    操作线程函数:从app.log文件中读取日志，并推送到服务器
    @param filelog(FileManage) 文件操作对象
    @param trans(JsonFormat) 处理字符串对象
    @param sendlog(SyslogManage) 日志发送对象
    @param cycle 推送周期,默认10秒
    """
    ######### 测试功能使用
    global count
    count = 0
    #filelog = FileManage()
    trans = JsonFormat()
    sendlog = SyslogManage(ip="127.0.0.1", port=514)
    #while True:
    # 从本地获得推送日志
    #list_log = filelog.get_log()
    # 遍历所有日志，进行格式解析和转换
    # 启明星辰防火墙
    #message = 'Sep 29 23:45:53 host SYSTEM_INFO: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" Content=""'
    # SYSTEM_INFO
    # message = ' Sep 29 23:45:53 host SYSTEM_INFO: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" Content="asdfa-asdfas_123asf asd1234" '
    # HA
    # message = ' Sep 29 23:45:53 host HA: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" Content="asdfa-asdfas_123asf asd1234" '
    # message = ' Sep 29 23:45:53 host HA: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" Action="asd" Content="asdfa-asdfas_123asf asd1234" '
    # OSPF
    # message = ' Sep 29 23:45:53 host OSPF: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" InterfaceName="sa" InterfaceAddr="sdf" EventHappen="asdf" OldState="sdfs" NewState="asdasd" Content="" '
    # message = 'Sep 29 23:45:53 host OSPF: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" InterfaceName="" InterfaceAddr="" SrcIP="" PacketType="" Content="" '
    # message = ' Sep 29 23:45:53 host OSPF: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" InterfaceName="" InterfaceAddr="" NBRRouterID="" EventHappen="" OldState="" NewState="" Content="" '
    # RIP
    # message = ' Sep 29 23:45:53 host RIP: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" Content="" '
    # DHCP
    # message = ' Sep 29 23:45:53 host DHCP: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" Content="" '
    # DNS_PROXY
    # message = ' Sep 29 23:45:53 host DNS_PROXY: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" DstIP="" Protocol="" SrcPort="" DstPort="" Content="" '
    # CONFIG
    # message = ' Sep 29 23:45:53 host CONFIG: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" UserName="" Operate="" Content="" '
    # message = ' Sep 29 23:45:53 host CONFIG: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" UserName="" Operate="" ManageStyle="" Content="" '
    # NAT
    # message = ' Sep 29 23:45:55 host NAT: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:55" NatType="" SrcIP="1.1.1.1" SrcPort="500" DstIP="2.2.2.2" DstPort="1000" BeforeTransAddr="" AfterTransAddr="" Protocol="" BeforeTransPort="" AfterTransPort="" PolicyID="" Content="" '
    # message = ' Jul 29 08:26:13 local Listening for Syslog messages on IP address: 172.16.1.150 '
    #message = '%s' % number
    #message = 'Jul 29 07:36:10 48.1.1.206 NAT: SerialNum=100022002000001807193963 GenTime="2019-07-29 15:34:12" NatType="Source NAT" SrcIP=20.1.1.11 SrcPort=137 DstIP=20.1.1.255 DstPort=137 BeforeTransAddr=20.1.1.11 AfterTransAddr=100.1.1.1 Protocol=UDP BeforeTransPort=137 AfterTransPort=137 PolicyID=1 Content="Session Backout" '
    # QOS
    # message = ' Sep 29 23:45:53 host QOS: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" DstIP="" Protocol="" SrcPort="" DstPort="" InInterface="" PolicyID="" Content="" '
    # APPCONTROL
    # message = ' Sep 29 23:45:53 host APPCONTROL: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" DstIP="" Protocol="" SrcPort="" DstPort="" InInterface="" AAA="" App="" Action="" PolicyID="" AppAction="" Content="" '
    # URL
    # message = ' Sep 29 23:45:53 host URL: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" DstIP="" Protocol="" SrcPort="" DstPort="" InInterface="" AAA="" Action="" PolicyID="" RuleID="" Url="" Content="" '
    # message = 'Jul 29 10:01:30 48.1.1.206 Jul 29 17:59:32 host URL: SerialNum=100022002000001807193963 GenTime="2019-07-29 17:59:32" SrcIP=45.1.1.202 DstIP=20.1.1.254 Protocol=HTTP SrcPort=49441 DstPort=80 InInterface=ge0/4 AAA=testt Action=DENY PolicyID=1 RuleID=1 Url=any Content="http://20.1.1.254/libs/bootstrap/js/bootstrap.min.js" '
    # SESSIONPOLICY
    # message = ' Sep 29 23:45:53 host SESSIONPOLICY: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" DstIP="" Protocol="" SrcPort="" DstPort="" InInterface="" PolicyID="" Content="" '
    # WEB_AUTH
    # message = ' Sep 29 23:45:53 host WEB_AUTH: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" UserName="" AuthResult="" Content="" '
    # FILTER
    # message = ' Sep 29 23:45:53 host FILTER: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" DstIP="" Protocol="" SrcPort="" DstPort="" InInterface="" OutInterface="" PolicyID="" Action="" Content="" '
    # message = ' Aug  8 16:45:08 LouCeng-FW-M FILTER: SerialNum=100021002000001905220898 GenTime="2019-08-08 16:45:08" SrcIP=10.53.164.87 DstIP=10.90.16.205 Protocol=TCP SrcPort=61093 DstPort=8080 PolicyID=64 Action=PERMIT Content="Session Setup" '
    #message = '  Aug  8 16:52:34 LouCeng-FW-M FILTER: SerialNum=100021002000001905220898 GenTime="2019-08-08 16:52:34" SrcIP=10.53.35.136 DstIP=10.90.16.205 Protocol=TCP SrcPort=58638 DstPort=8080 PolicyID=64 Action=PERMIT Content="Session Backout" '
    # message = '  Aug  8 16:52:04 host FILTER: SerialNum=100021002000001905220572 GenTime="2019-08-08 16:52:04" SrcIP=10.2.255.142 DstIP=10.2.253.101 Protocol=TCP SrcPort=34392 DstPort=50001 InInterface=ge0/3 OutInterface=ge0/2 PolicyID=6 Action=DENY Content="The packet was blocked because the firewall policy is deny"'
    # FLOOD
    # message = ' Sep 29 23:45:53 host FLOOD: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" DstIP="" Protocol="" SrcPort="" DstPort="" InInterface="" PolicyID="" Action="" Content="" '
    # SCAN
    # message = ' Sep 29 23:45:53 host SCAN: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" InInterface="" Content="" '
    # AV
    #message = ' Sep  29 23:45:53 LouCeng-FW-M AV: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" DstIP="" Protocol="" SrcPort="" DstPort="" Action="" Content="" '
    # IPS
    #message = ' Sep 29 23:45:53 host IPS: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" DstIP="" Protocol="" SrcPort="" DstPort="" PolicyID="" Action="" Content="" '
    # WEB_SECURITY
    # message = ' Sep 29 23:45:53 host WEB_SECURITY: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" DstIP="" Protocol="" SrcPort="" DstPort="" InInterface="" Action="" PolicyName="" Content="" '
    # THREAT_INTELLIGENCE
    # message = ' Sep 29 23:45:53 host THREAT_INTELLIGENCE: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" DstIP="" Protocol="" SrcPort="" DstPort="" InInterface="" Action="" PolicyName="" Content="" '
    # DDOS
    # message = ' Sep 29 23:45:53 host DDOS: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" DstIP="" InInterface="" Content="" '
    # ANTIARP
    # message = ' Sep 29 23:45:53 host ANTIARP: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" SrcPort="" SMAC="" Protocol="" Content="" '
    # message = ' Sep 29 23:45:53 host ANTIARP: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" SMAC="" Protocol="" Content="" '
    # BLACKLIST
    # message = ' Sep 29 23:45:53 host BLACKLIST: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" DstIP="" Protocol="" SrcPort="" DstPort="" Content="" '
    # IPSECVPN
    # message = ' Sep 29 23:45:53 host IPSECVPN: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" DstIP="" Content="" '
    # L2TPVPN
    # message = ' Sep 29 23:45:53 host L2TPVPN: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" DstIP="" DstPort="" UserName="" Content="" '
    # SSLVPN
    # message = ' Sep 29 23:45:53 host SSLVPN: SerialNum=001000000000001309109941 GenTime="2012-09-29 23:45:53" SrcIP="" DstIP="" DstPort="" UserName="" Content="" '

    # 山石防火墙
    # EVENT
    # message = ' 2019-07-30 12:50:20, WARNING@MGMT: Admin user "hillstone" logined through SSH, and the IP is 172.16.30.4. '
    #message = 'Aug  7 18:52:22 2812008195003847(root) 4424362f Event@MGMT: Admin user "hillstone" logined through SSH, from 172.16.30.4:62434 to 172.16.100.100:22(TCP) '
    # ALARM
    #message = ' 2019-03-11 15:11:40, CRIT@MGMT: The IP 10.45.0.67 has failed 3 times to login through HTTPS and will be locked for 2 minutes '
    #message = 'Aug  7 18:52:22 2812008195003847(root) 4424362f Traffic@FLOW: SESSION: 172.19.112.25:64141->10.11.183.156:445(TCP), interface ethernet0/1, vr trust-vr, policy 16, user -@-, host -, mac 0000.0000.0000, policy deny\\n\\u0000'

    # 网闸
    #message = 'charset=[UTF-8] logType=[systemService] serviceName=[TCP\\u901a\\u7528\\u4e2d\\u8f6c\\u670d\\u52a1] desc=[\\u8fde\\u63a5\\u5931\\u8d25[10.1.56.174:22999][Connection refused: /10.1.56.174:22999]] result=[\\u5931\\u8d25] date=[2019-11-05 10:21:55.830]'
    # 镇关
    message = 'Dec 27 03:48:53 ADSL APP_POLICY: SerialNum=71049-0139Y-27009-J368P-J8H1I GenTime=\"2019-12-27 03:48:53\" UserID=2 UserName=10.255.8.61 SrcIP=10.255.8.61 DstIP=95.179.216.230 Protocol=TCP SrcPort=64711 DstPort=80 SrcMac=74:9d:8f:06:c2:7e DstMac=b4:43:26:8d:4a:f1 PolicyId=2 RuleID=1 AppName=Ammyy-Admin AppAction=\u64cd\u4f5c Action=deny'



    list_log = [message]
    for each in list_log:
        #sendlog.message = trans.transform_str(each)
        sendlog.message = each
        # 跳过空字符串
        if len(sendlog.message) == 0:
            continue
        sendlog.facility = trans.facility
        sendlog.severity = trans.severity
        sendlog.push_log_to_server()
        count = count + 1
    # 等待时间：推送周期
    #time.sleep(int(cycle))
    ############ 测试功能使用
    # filelog.currline = 0


def set_cycle():
    """从配置文件获取推送周期"""
    # obj = ConfigParser.ConfigParser()
    # obj.read(CONF_PATH)
    return 1000 #obj.get("SYSLOG", "time")


def main():
    """
    对于每一个推送功能创建对应的线程
    """
    # 线程列表
    # thread_list = list()
    # logthread_id = thread.start_new_thread(logthread, (set_cycle(),))
    # thread_list.append(logthread_id)
    ############ 测试功能使用
    # try:
    #for i in range(1,5000):
    logthread(set_cycle(), 1)
    
    #while True:
    #    pass
    # except KeyboardInterrupt:
        # print "发送日志条数:",count


if __name__ == '__main__':
    main()

