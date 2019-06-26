#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/6/21 9:39
# @Author  : IWC
# @Param   : 
# @File    : RunSQL.py


import DBConnect
import LogUtil
import VariableUtil
import os, sys

name = os.path.basename(__file__)
log = LogUtil.Logger(name)


def runSQL(conn, sql):
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
        log.info('Executor SQL sucessful')
        cur.close()
        conn.commit()
        conn.close()
        log.info('Close database connection..')
    return result
