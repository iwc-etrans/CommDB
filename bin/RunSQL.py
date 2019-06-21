#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/6/21 9:39
# @Author  : Fcvane
# @Param   : 
# @File    : RunSQL.py


import DBConnect
import LogUtil
import VariableUtil
import os, sys

name = os.path.basename(__file__)
log = LogUtil.Logger(name)

def runSQL(sql):
    dbType, conn = DBConnect.getConnect('SCOTT_10.45.15.201')
    log.info('Open database connection')
    cur = conn.cursor()
    result = ""
    try:
        cur.execute(sql)
        result = cur.fetchall()
        log.info('Start to executor SQL ..')
    except Exception as e:
        log.error('Executor SQL failure..')
        sys.exit()
    finally:
        log.info('Executor SQL sucessfu.l')
        cur.close()
        conn.commit()
        conn.close()
        log.info('Close database connection..')
    return result
