#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/6/13 8:48
# @Author  : Fcvane
# @Param   : 日志通用方法
# @File    : LogUtil.py

import os
import datetime
import logging.handlers
import VariableUtil

class Logger(logging.Logger):
    def __init__(self, name):
        super(Logger, self).__init__(self)
        self.name = name

        LOG_PATH = VariableUtil.LOG_PATH
        if os.path.exists(LOG_PATH):
            1
        else:
            os.mkdir(LOG_PATH)
        currDate = datetime.datetime.now().strftime('%Y-%m-%d')
        logFile = LOG_PATH + os.sep + 'CommDBLog_{currDate}.log'.format(currDate=currDate)

        fh = logging.FileHandler(logFile, mode='a')
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '[ %(asctime)s ] - [ %(filename)15s ] - [ line:%(lineno)5d ] - %(levelname)5s : %(message)s', )
        # datefmt='%Y-%m-%d %H:%M:%S')
        # 获取logger名称
        logger = logging.getLogger()
        # 设置日志级别
        logger.setLevel(logging.INFO)

        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        # 避免日志重复
        if not logger.handlers:
            self.addHandler(fh)
            # 控制台打印
            self.addHandler(ch)

        # # name, description, flag
        # # if flag == "info":
        # #     logger.info(description)
        # # elif flag == "warning":
        # #     logger.warning(description)
        # # elif flag == "error":
        # #     logger.error(description)
        # # else:
        # #     logger.error("parameter error, please check and try again .")
        # # 日志接口，用户只需调用这里的接口即可，这里只定位了INFO, WARNING, ERROR三个级别的日志，可根据需要定义更多接口
        # @classmethod
        # def info(cls, msg, name):
        #     cls(name).logger.info(msg)
        #     return
        #
        # @classmethod
        # def warning(cls, msg, name):
        #     cls(name).logger.warning(msg)
        #     return
        #
        # @classmethod
        # def error(cls, msg):
        #     cls.logger.error(msg)
        #     return
