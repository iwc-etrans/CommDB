#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/6/13 8:52
# @Author  : Fcvane
# @Param   : 全局变量
# @File    : VariableUtil.py

import os

#python脚本绝对路径
FILE_PATH = os.path.realpath(__file__)
#脚本路径
BIN_PATH = os.path.split(FILE_PATH)[0]
#配置文件路径
CONF_PATH = os.path.abspath(BIN_PATH + '/../conf')
#数据库配置路径
DB_PATH = os.path.abspath(BIN_PATH + '/../conf/dbParams')
#日志文件路径
LOG_PATH = os.path.abspath(BIN_PATH + '/../log')
#临时文件目录
TMP_PATH = os.path.abspath(BIN_PATH + '/../tmp')
#目标文件目录
RLT_PATH = os.path.abspath(BIN_PATH + '/../result')
