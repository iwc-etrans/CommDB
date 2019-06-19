#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/6/12 16:24
# @Author  : Fcvane
# @Param   : 
# @File    : DbConnect.py


import os, sys
import datetime
import logging
import cx_Oracle
import psycopg2
import pymysql
import VariableUtil
from XmlUtil import dbCFGInfo

try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree


# 获取数据库连接
def getConnect(program):
    config = dbCFGInfo(program)
    print (config , "-----------------")
    for cronName in config.keys():
        dbType = cronName
        cronConfig = config[cronName]
        if cronConfig['enable'] == 'Ture':
            host = cronConfig['host']
            port = cronConfig['port']
            sid = cronConfig['sid']
            serverName = cronConfig['serverName']
            userName = cronConfig['userName']
            passWord = cronConfig['passWord']
            print(dbType, host, port, sid, serverName, userName, passWord)
            try:
                if dbType == 'oracle':
                    if sid is None or sid == '':
                        # logger.error ('sid not exists , the %s config is error !!.'%program)
                        sys.exit()
                    else:
                        # logger.info('exec connect oracle start ...')
                        connect = cx_Oracle.connect(userName, passWord,
                                                    '{host}:{port}/{sid}'.format(host=host, port=port, sid=sid))
                        # logger.info('connect oracle sucessful !.')
                elif dbType == 'mysql':
                    if serverName is None or sid == '':
                        # logger.error('serverName not exists , the %s config is error !!.' % program)
                        sys.exit()
                    else:
                        # logger.info('exec connect mysql start ...')
                        connect = pymysql.connect(host=host, port=int(port), user=userName, passwd=passWord,
                                                  db=serverName, charset='utf8')
                        # logger.info('connect mysql sucessful !.')
                elif dbType == 'postgresql':
                    # logger.info('exec connect postgresql start ...')
                    connect = psycopg2.connect(database=serverName, user=userName, password=passWord, host=host,
                                               port=port)
                    # logger.info('connect postgresql sucessful !.')
            except:
                print(1111111)
                # logger.error('connect to db error !!.')
                # logger.exception()
            return connect


if __name__ == '__main__':
    # conn = getConnect('fircus_dkh')
    # cur = conn.cursor()
    # cur.execute('select * from ttable_0409')
    # result = cur.fetchall()
    # print (result)

    conn = getConnect('commondb')
    cur = conn.cursor()
    cur.execute("select 1+1 from dual")
    cur.close()
    conn.commit()
    conn.close()
