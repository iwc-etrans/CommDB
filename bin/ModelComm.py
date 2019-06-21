#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/6/21 8:50
# @Author  : Fcvane
# @Param   : 模型比对
# @File    : ModelComm.py

import DBConnect
import LogUtil
import os
import XmlUtil

name = os.path.basename(__file__)
log = LogUtil.Logger(name)


def columnComm(tabname):
    dbType, conn = DBConnect.getConnect('SCOTT_10.45.15.201')
    if dbType == 'ORACLE':
        log.info("DB type is ORACLE, Start get columns...")
        sql = XmlUtil.dbSQL('COLUMN')
        sqlformat=sql.format(tabname=tabname)
        cur = conn.cursor()
        cur.execute(sqlformat)
        result = cur.fetchall()
        print(result)
        cur.close()
        conn.commit()
        conn.close()


columnComm('EMP')
