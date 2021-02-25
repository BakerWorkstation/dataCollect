#!/usr/bin/env python
# -*- coding:utf-8 -*-

from queue import Queue
from bin.handle_file import ResultHandleMixin
from bin.find_file import FileLoader
from bin.metadata import Metadata
MAX_QUEUE_SIZE = 100





#初始化队列
task_q = Queue(MAX_QUEUE_SIZE)
completed_q = Queue(MAX_QUEUE_SIZE)

#开启文件查找线程
thread_file_loader = FileLoader(task_q, completed_q)
thread_file_loader.start()

    #开启文件处理线程
thread_data_importer = ResultHandleMixin(task_q, completed_q)
thread_data_importer.start()


