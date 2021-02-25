#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import queue
import threading
import time
import logging
from conf.config_t import handle_conf
import heapq
class FileLoader(threading.Thread):
    __name = 'FileLoader'

    def __init__(self,task_q,completed_q): #一个任务队列，一个任务完成队列
        super(FileLoader, self).__init__(name=self.__name)
        self.SOU_DIR = eval(handle_conf.get("handle_file", "SOU_DIR"))
        self.file_dir = self.SOU_DIR
        self.task_queue = task_q
        self.completed_queue = completed_q
        self.intaskfiles = [] #正在导入或等待导入的文件信息列表
        print("find_file start")
        #self.logger = self.setup_logging()

    #设置日志
    def setup_logging(self) -> logging.Logger:
        """
        用于设置日志格式
        :return:
        """
        logger = logging.getLogger(self.__name)
        logger.setLevel(logging.INFO)
        f_handler = logging.FileHandler('/home/mhp/ftp_test/pyftpdlib/log/' + self.__name +'_log.log')
        f_handler.setLevel(logging.INFO)
        
        formatter = logging.Formatter('%(asctime)s  - %(levelname)s - %(message)s')
        f_handler.setFormatter(formatter)
        logger.addHandler(f_handler)
        return logger

    def _loadfile2queue(self):
        """
        用于对文件进行查找
        :return:
        """
        '''
        while True:
            try:
                #获取不到完成的队列任务抛出异常
                completedfile = self.completed_queue.get(block=False)
                self.intaskfiles.remove(completedfile)
            except queue.Empty:
                break
        '''
        '''
        dir_ 为当前正在遍历的这个文件夹的本身地址
        _ 不需要的信息 该文件驾中所有的目录的名字
        files 该文件夹中所有的文件
        '''
        print('------------')

        for dir_,_,files in os.walk(self.file_dir):
            for filename in files:
                if filename in self.intaskfiles:
                    continue
                file_path = os.path.join(dir_,filename)
                #只取最后访问时间在20秒之前的文件，避免文件还未导入完成就被处理了
                if time.time() -os.stat(file_path).st_atime >20:
                    if self.task_queue.full():
                        #加日志输出
                        pass
                    if not self.task_queue.full():
                        self.task_queue.put(file_path,block = False)
                       # print(filename)
                        self.intaskfiles.append(filename)
                        #self.logger.info('<把文件：%s加入消息队列中>',file_path)


    def run(self):
        """
        程序启动函数
        :return:
        """

        while True:

            try:
                #死循环
                self._loadfile2queue()
            except Exception as e:
                time.sleep(20)
            time.sleep(3)

