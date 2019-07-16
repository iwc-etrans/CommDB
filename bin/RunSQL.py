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
        log.info('Start to executor SQL')
    except Exception as e:
        log.error('Executor SQL failure')
        sys.exit()
    finally:
        log.info('Executor SQL sucessful')
        cur.close()
        conn.commit()
        conn.close()
        log.info('Close database connection')
    return result


def ddlSQL(conn, sql):
    log.info('Open database connection')
    cur = conn.cursor()
    try:
        cur.execute(sql)
        log.info('Start to executor DDL SQL')
    except Exception as e:
        log.error('Executor DDL SQL failure')
        sys.exit()
    finally:
        log.info('Executor DDL SQL sucessful')
        cur.close()
        conn.close()
        log.info('Close database connection')


def dmlManySQL(conn, sql, array=[]):
    log.info('Open database connection')
    cur = conn.cursor()
    try:
        cur.executemany(sql, array)
        log.info('Start to executor DML SQL')
    except Exception as e:
        log.error('Executor DML SQL failure')
        sys.exit()
    finally:
        log.info('Executor DML SQL sucessful')
        cur.close()
        conn.commit()
        conn.close()
        log.info('Close database connection')
