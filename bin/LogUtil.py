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


def log(name, description, flag):
    LOG_PATH = VariableUtil.LOG_PATH
    currDate = datetime.datetime.now().strftime('%Y-%m-%d')
    logFile = LOG_PATH + os.sep + 'CommDBLog_{currDate}.log'.format(currDate=currDate)

    fh = logging.FileHandler(logFile, mode='a')
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s  - %(name)15s - %(levelname)8s : %(message)s', )
    # datefmt='%Y-%m-%d %H:%M:%S')
    # 获取logger名称
    logger = logging.getLogger(name)
    # 设置日志级别
    logger.setLevel(logging.INFO)

    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # 避免日志重复
    if not logger.handlers:
        logger.addHandler(fh)
        # 控制台打印
        logger.addHandler(ch)

    if flag == "info":
        logger.info(description)
    elif flag == "warning":
        logger.warning(description)
    elif flag == "error":
        logger.error(description)
    else:
        logger.error("parameter error, please check and try again .")
