#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/6/12 16:24
# @Author  : IWC
# @Param   : 数据库连接
# @File    : DBConnect.py


import os, sys
import cx_Oracle
import psycopg2
import pymysql
import LogUtil
from impala.dbapi import connect as impc
from XmlUtil import dbCFGInfo

try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree

name = os.path.basename(__file__)
log = LogUtil.Logger(name)


def getDBinfo(auth):
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
        if dbType == 'ORACLE':
            if serverName is None or serverName == '':
                if sid is not None and sid != '':
                    db_cfg = ['0', sid]
                else:
                    log.error(
                        'Wrong db config in db_commondb.xml with auth :{authname} ,servername and sid is null'.format(
                            authname=auth))
                    sys.exit()
            else:
                db_cfg = ['1', serverName]
        elif dbType == 'MYSQL':
            if serverName is not None and serverName != '':
                db_cfg = serverName
            else:
                log.error('Wrong db config in db_commondb.xml with auth :{authname} ,servername is null'.format(
                    authname=auth))
                sys.exit()
        elif dbType == 'POSTGRESQL':
            if serverName is not None and serverName != '':
                db_cfg = serverName
            else:
                log.error('Wrong db config in db_commondb.xml with auth :{authname} ,servername is null'.format(
                    authname=auth))
                sys.exit()
        elif dbType == 'IMPALA_KUDU':
            if serverName is not None and serverName != '':
                db_cfg = serverName
            else:
                log.error('Wrong db config in db_commondb.xml with auth :{authname} ,servername is null'.format(
                    authname=auth))
                sys.exit()
        else:
            log.error('Wrong db config in db_commondb.xml with auth :{authname} ,wrong dbtype'.format(authname=auth))
            sys.exit()
        db_config = {'db_type': dbType, 'db_host': host, 'db_port': port, 'db_cfg': db_cfg,
                     'db_username': userName, 'db_password': passWord}
        return db_config
    else:
        log.error('DB config is not available in db_commondb.xml with auth :{authname}'.format(authname=auth))
        sys.exit()


def getConnect(auth):
    db_config = getDBinfo(auth)
    dbType = db_config['db_type']
    host = db_config['db_host']
    port = db_config['db_port']
    userName = db_config['db_username']
    passWord = db_config['db_password']
    db_cfg = db_config['db_cfg']
    try:
        if dbType == 'ORACLE':
            log.info('Exec connect oracle start ...')
            if db_cfg[0] == '0':
                dsn = cx_Oracle.makedsn(host, port, db_cfg[1])
            elif db_cfg[0] == '1':
                dsn = cx_Oracle.makedsn(host, port, service_name=db_cfg[1])
            connect = cx_Oracle.connect(userName, passWord, dsn)
            log.info('Connect oracle sucessful')
        elif dbType == 'MYSQL':
            log.info('Exec connect mysql start ...')
            connect = pymysql.connect(host=host, port=int(port), user=userName, passwd=passWord, db=db_cfg,
                                      charset='utf8')
            log.info('Connect mysql sucessful')
        elif dbType == 'POSTGRESQL':
            log.info('Exec connect postgresql start ...')
            connect = psycopg2.connect(database=db_cfg, user=userName, password=passWord, host=host, port=port)
            log.info('Connect postgresql sucessful')
        elif dbType == 'IMPALA_KUDU':
            log.info('Exec connect impala kudu start ...')
            connect = impc(host=host, port=int(port),database=db_cfg)
            log.info('Connect impala kudu sucessful')
    except Exception as e:
        log.info('Connect to db failure ,please check config and try again ..')
        sys.exit()
    return [dbType, connect]


if __name__ == '__main__':
    dbType, conn = getConnect('targetDB')
    cur = conn.cursor()
    #cur.execute("show tables;")
    cur.execute("select SNO,SNAME,SAGE,SDEPT from STUDENT where SNO in ('1','2','3');")
    result = cur.fetchall()
    print(result)
    cur.close()
    conn.commit()
    conn.close()
