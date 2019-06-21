#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/6/12 16:24
# @Author  : Fcvane
# @Param   : 数据库连接
# @File    : DBConnect.py


import os, sys
import cx_Oracle
import psycopg2
import pymysql
import LogUtil
from XmlUtil import dbCFGInfo

try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree

name = os.path.basename(__file__)
log = LogUtil.Logger(name)

# 获取数据库连接
def getConnect(auth):
    config = dbCFGInfo(auth)
    cronConfig = config[auth]
    if cronConfig['enable'] == 'Ture':
        dbType = cronConfig['dbType']
        host = cronConfig['host']
        port = cronConfig['port']
        sid = cronConfig['sid']
        serverName = cronConfig['serverName']
        userName = cronConfig['userName']
        passWord = cronConfig['passWord']
        # print(dbType, host, port, sid, serverName, userName, passWord)
        try:
            if dbType == 'ORACLE':
                if sid is None or sid == '':
                    log.error('Sid not exists , the %s config is error'.format(program=auth))
                    sys.exit()
                else:
                    log.info('Exec connect oracle start ...')
                    # print(" cx_Oracle.connect({userName}, {passWord},'{host}:{port}/{sid}')".format(userName=userName,
                    #                                                                                 passWord=passWord,
                    #                                                                                 host=host,
                    #                                                                                 port=port, sid=sid))
                    connect = cx_Oracle.connect(userName, passWord,
                                                '{host}:{port}/{sid}'.format(host=host, port=port, sid=sid))
                    log.info('Connect oracle sucessful')
            elif dbType == 'MYSQL':
                if serverName is None or sid == '':
                    log.error('ServerName not exists , the %s config is error !!'.format(program=auth))
                    sys.exit()
                else:
                    log.info('Exec connect mysql start ...')
                    connect = pymysql.connect(host=host, port=int(port), user=userName, passwd=passWord,
                                              db=serverName, charset='utf8')
                    log.info('Connect mysql sucessful.')
            elif dbType == 'POSTGRESQL':
                log.info('Exec connect postgresql start ...')
                connect = psycopg2.connect(database=serverName, user=userName, password=passWord, host=host,
                                           port=port)
                log.info('Connect postgresql sucessful.')
        except Exception as e:
            log.info('Connect to db failure ,please check config and try again ..')
            connect = ""
        return [dbType, connect]


if __name__ == '__main__':
    dbType, conn = getConnect('SCOTT_10.45.15.201')
    cur = conn.cursor()
    cur.execute("select 1+1 from dual")
    result = cur.fetchall()
    print(result)
    cur.close()
    conn.commit()
    conn.close()
