#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import shutil
import time
#from file_split import Split
import logging
import threading
from conf.config_t import handle_conf
from bin import dns_channel
import xml.dom.minidom
import xml.etree.cElementTree as ET
#from Habse_operation import Hbase
import asyncio

class ResultHandleMixin(threading.Thread):

    name = 'ResultHandleMixin'
    def __init__(self,task_q,complete_q):
        super(ResultHandleMixin, self).__init__(name=self.name)
        self.DST_DIR = eval(handle_conf.get("handle_file", "DST_DIR"))
        self.task_queue = task_q
        self.complete_queue =complete_q
        self.doit = dns_channel.DetectDomainTunnel()
        self.doit.init('./lib/domain_tunnel.so', b'db/')
        self.dict_t =dict()
        self.xml_dict=dict()
        #self.Split_t = Split()
       # self.logger = self.setup_logging()
        self.dst_dir = self.DST_DIR
        print('File_processing thread on')
        '''
        self.Hbase_t = Hbase()
        try:
            self.Hbase_t.create_table('test_table','fd1')
        except Exception as e:
            self.Hbase_t.connect_table('test_table')
        '''

    #设置日志

    def setup_logging(self) ->logging.Logger:
        """
        参数：无
        :return:一个日志类的句柄
        """
        logger = logging.getLogger(self.name)
        logger.setLevel(logging.INFO)
        f_handler = logging.FileHandler('/home/mhp/ftp_test/pyftpdlib/log/' + self.name +'_log.log')
        f_handler.setLevel(logging.INFO)
        
        formatter = logging.Formatter('%(asctime)s  - %(levelname)s - %(message)s')
        f_handler.setFormatter(formatter)
        logger.addHandler(f_handler)
        return logger

    #用于移动文件
    def _move_file(self,file_path:str,dst_dir:str):
        """
        用于对文件进行移动
        :param file_path:目标文件
        :param dst_dir:目标文件路径
        :return:无
        """


        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)
        filename = os.path.split(file_path)[1]
        #如果该路径存在该文件就移除原路径下的文件
        if os.path.exists(os.path.join(dst_dir,filename)):
            os.remove(file_path)

        else:
            shutil.move(file_path,dst_dir)

    def _copy_File(self,file_path:str,dst_dir:str):
        """
        拷贝文件到指定路径下
        :param file_path:
        :param dst_dir:
        :return:
        """
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)
        filename = os.path.split(file_path)[1]
        #if os.path.exists(os.path.join(dst_dir,filename)):
        #   os.remove(file_path)
        try:
            shutil.copyfile(file_path,os.path.join(dst_dir,filename))

        except Exception as e:
            print(e)
            #self.logger.error(e, exc_info=True)

        #shutil.copyfile(file_path,dst_dir)


    def remov_file(self,file_path:str):
        """

        :param file_path:
        :return:
        """
        try:
            os.remove(file_path)
            #self.logger.debug('<文件路径：%s 文件已移除>',file_path)

        except Exception as e:
            pass
            #self.logger.error(e,exc_info = True)

    def handle_result(self,file_path:str):

        #对文件进行校验
        print(file_path)
        # self._copy_File(file_path,self.dst_dir)

    #未完成功能：每读写500行，调用handle_file_habse
    # def handle_file_kafka(self,file_path:str):
    #     with open(file_path,'rb') as f:
    #         i=1
    #         for line in f:
    #             '''
    #             i+=1
    #             if i==500:
    #                 pass
    #             '''
                
                #self.Split_t.send_kafka(line)
            #self.logger.info('<文件路径：%s 文件流已写入kafka>',file_path)


     #未完成函数，没有实现向hbase中传送数据

    def judging_file_type(self,file_path:str):
        """
        通过后缀判断文件类型，并作相应处理
        :param file_path:
        :return:
        """
        filename = os.path.split(file_path)[1]
        filename_list = filename.split('.')[-1]
        if filename_list=='xml':
            print(file_path)
            self.handle_xml(file_path)
        elif filename_list=='pcap':
            print(file_path)
            self.handle_pcap(file_path)

    def handle_xml(self,file_path:str):
        filename = os.path.split(file_path)[1]
        dst_dir= self.dst_dir + filename
        self._copy_File(file_path, self.dst_dir)
        dom = xml.dom.minidom.parse(file_path)
        root = dom.documentElement
        bb = root.getElementsByTagName('FILENAME')
        FILENAME=bb[0].firstChild.data
        bb = root.getElementsByTagName('LINENO')
        LINENO=bb[0].firstChild.data
        bb = root.getElementsByTagName('TASKID')
        TASKID=bb[0].firstChild.data
        bb = root.getElementsByTagName('GROUPID')
        GROUPID = bb[0].firstChild.data
        bb = root.getElementsByTagName('DANWEI')
        DANWEI = bb[0].firstChild.data

        bb = root.getElementsByTagName('RULEID')
        RULEID = bb[0].firstChild.data

        bb = root.getElementsByTagName('TASKTIME')
        TASKTIME = bb[0].firstChild.data

        filename = os.path.split(file_path)[1]
        self.xml_dict['FILENAME']=FILENAME
        self.xml_dict['LINENO'] = LINENO
        self.xml_dict['TASKID'] = TASKID
        self.xml_dict['GROUPID'] = GROUPID
        self.xml_dict['TASKTIME'] = TASKTIME
        self.xml_dict['RULEID'] = RULEID
        self.xml_dict['DANWEI'] = DANWEI

        key = filename.split('.')[0]
        #如果没有该主键，则建立该主键，并把数据存入字典中
        #如果有主键存在，取出该主键的值，调用PTD接口
        if  self.dict_t.get(key)==None:
            self.dict_t.setdefault(key,self.xml_dict)
        else:
            pcap_path=self.dict_t.setdefault(key, self.xml_dict)
            #移动地址后再进行处理

            self.xml_dict['FILENAME'] = pcap_path
            self.doit.detect(pcap_path=bytes(pcap_path,encoding="utf8"),xml_dict=self.xml_dict)


            #
            # root = dom.documentElement
            #
            # print(root.DOCUMENT_FRAGMENT_NODE)
        # tree = ET.ElementTree(file=file_path)
        # root=tree.getroot()
        # for elem in tree.iter(tag='catalog'):
        #     print(elem.tag, elem.attrib)


    def handle_pcap(self,file_path:str):
        #复制pcap包到指定路径下
        filename = os.path.split(file_path)[1]
        dst_dir= self.dst_dir + filename
        self._copy_File(file_path,self.dst_dir)



        pcap_new_path =self.dst_dir + filename

        #获得存入字典的主键
        key = filename.split('.')[0]
        if  self.dict_t.get(key)==None:
            self.dict_t.setdefault(key,pcap_new_path)
        else:
            self.xml_dict=self.dict_t.setdefault(key,pcap_new_path)
            #移动地址后再进行处理
            self.xml_dict['FILENAME'] = pcap_new_path
            self.doit.detect(pcap_path=bytes(pcap_new_path,encoding="utf8"),xml_dict=self.xml_dict)

        #如果字典中没有该主键，则把pcap包数据存入




    def handle_file_habase(self,file_path:str,value:bytes):
        filename = os.path.split(file_path)[1]
        #self.Hbase_t.put_data_binaryvalues()





    def run(self):
        while True:
            try:
                if self.task_queue.empty():
                    pass
                    #添加日志
                #队列为空时堵塞阻塞等待
                file_path = self.task_queue.get()

                #对文件处理函数
                #self.judging_file_type(file_path)

                #暂时不做操作
                #self.handle_result(file_path)


                #向已完成队列写入已完成的导入文件信息(队列已时阻塞等待)
                #complatedfile = os.path.split(file_path)[1]
                #self.complete_queue.put(complatedfile)
            except Exception as e:
                print(e)
                #捕获异常写入日志
               # time.sleep(60)
                #self.logger.error('<程序报错停止60秒>',exc_info =True)


if __name__ =="__main__":
    a =ResultHandleMixin(1,2)
    a.handle_xml(r'C:\Users\AT\Desktop\file\1.xml')
                





    
        
