#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/6/21 8:50
# @Author  : Fcvane
# @Param   : 模型比对
# @File    : ModelComm.py

import DBConnect
import LogUtil
import VariableUtil
import os

name = os.path.basename(__file__)


def columnComm(tabname):
    dbType, conn = DBConnect.getConnect('SCOTT_10.45.15.201')
    if dbType == 'ORACLE':
        LogUtil.log(name, "DB type is ORACLE, Start get columns...", "info")
        sql = ""
