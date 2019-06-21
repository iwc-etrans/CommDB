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

# 获取数据库连接
def getConnect(program):
    config = dbCFGInfo(program)
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
                        LogUtil.log(name, 'Sid not exists , the %s config is error'.format(program=program), 'error')
                        sys.exit()
                    else:
                        LogUtil.log(name, 'Exec connect oracle start ...', 'info')
                        print (" cx_Oracle.connect({userName}, {passWord},'{host}:{port}/{sid}')".format(userName=userName,passWord=passWord,host=host, port=port, sid=sid))
                        connect = cx_Oracle.connect(userName, passWord,
                                                    '{host}:{port}/{sid}'.format(host=host, port=port, sid=sid))
                        LogUtil.log(name, 'Connect oracle sucessful !', 'info')
                elif dbType == 'mysql':
                    if serverName is None or sid == '':
                        LogUtil.log(name, 'ServerName not exists , the %s config is error !!'.format(program=program), 'error')
                        sys.exit()
                    else:
                        LogUtil.log(name, 'Exec connect mysql start ...','info')
                        connect = pymysql.connect(host=host, port=int(port), user=userName, passwd=passWord,
                                                  db=serverName, charset='utf8')
                        LogUtil.log(name, 'Connect mysql sucessful !', 'info')
                elif dbType == 'postgresql':
                    LogUtil.log(name, 'Exec connect postgresql start ...', 'info')
                    connect = psycopg2.connect(database=serverName, user=userName, password=passWord, host=host,
                                               port=port)
                    LogUtil.log(name, 'Connect postgresql sucessful !', 'info')
            except:
                LogUtil.log(name, 'Connect to db failure ,please check config and try again !!', 'error')
                connect = ""
            return connect


if __name__ == '__main__':

    conn = getConnect('commondb')
    cur = conn.cursor()
    cur.execute("select 1+1 from dual")
    result = cur.fetchall()
    print(result)
    cur.close()
    conn.commit()
    conn.close()
