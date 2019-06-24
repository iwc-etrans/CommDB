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


def commpare(dict={}):
    a = dict['SCOTT_10.45.15.201']
    b = dict['SCOTT_10.45.15.205']
    if len(list(set(b) - set(a))) == 0 and len(list(set(a) - set(b))) == 0:
        log.info("Table field and type same")
    else:
        log.info("Table field and type different")
        if len(list(set(b) - set(a))) > 0:
            log.info("Elements in SCOTT_10.45.15.205 but not in SCOTT_10.45.15.201 ,detail:",
                     ''.join(list(set(b) - set(a))))
        elif len(list(set(a) - set(b))) > 0:
            log.info("Elements in SCOTT_10.45.15.201 but not in SCOTT_10.45.15.205 ,detail:",
                     ''.join(list(set(a) - set(b))))


def columnComm(tabname):
    dbrange = ['SCOTT_10.45.15.201', 'SCOTT_10.45.15.205']  # 参数
    dict = {}
    for i in dbrange:
        dbType, conn = DBConnect.getConnect(i)
        # 暂时处理ORACLE
        if dbType == 'ORACLE':
            log.info("DB type is ORACLE, Start get columns...")
            sql = XmlUtil.dbExeSQL('COLUMN')
            sqlformat = sql.format(tabname=tabname)
            cur = conn.cursor()
            cur.execute(sqlformat)
            result = cur.fetchall()
            # 数据处理
            array = []
            for j in result:
                array.append(j[0])
            dict[i] = array
            cur.close()
            conn.commit()
            conn.close()
    return dict


dict = columnComm('EMP')
commpare(dict)
