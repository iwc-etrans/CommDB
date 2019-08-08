#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/6/21 8:50
# @Author  : IWC
# @Param   : 数据比对
# @File    : DataComm.py

import DBConnect
import LogUtil
import os
import XmlUtil
import RunSQL
import VariableUtil
from datetime import datetime
import time

name = os.path.basename(__file__)
log = LogUtil.Logger(name)


def getPKColumn(auth):
    dbType, conn = DBConnect.getConnect(auth)
    # 暂时只处理ORACLE
    dir_pk = {}
    if dbType == 'ORACLE':
        log.info('DB type is ORACLE, Start get all primary key...')
        sql = "select b.TABLE_NAME,listagg(b.COLUMN_NAME, ',') within group (order by b.POSITION) as pkcol from USER_CONSTRAINTS a,USER_CONS_COLUMNS b where a.TABLE_NAME = b.TABLE_NAME and a.CONSTRAINT_TYPE = \'P\' and a.STATUS = \'ENABLED\' group by b.table_name "
        # 获取所有状态为enabled 的主键表名及主键字段
        result = RunSQL.runSQL(conn, sql)
        for arr in result:
            dir_pk[arr[0]] = arr[1]
        log.debug("All primary key is %s", dir_pk)
        return dir_pk


def getInsertrecord(sourcedb, targetdb, tablename):
    source_dbType, source_conn = DBConnect.getConnect(sourcedb)
    target_dbType, target_conn = DBConnect.getConnect(targetdb)
    pk_arr = getPKColumn(sourcedb)
    tablename = str.upper(tablename)
    if tablename in pk_arr.keys():
        pk_cols = pk_arr[tablename]
        log.info("Find table : %s primary key cols is %s", tablename, pk_cols)
        source_cur = source_conn.cursor()
        sql_get_source_data = "select %(cols)s from %(table)s" % {'cols': pk_cols, 'table': tablename}
        source_cur.execute(sql_get_source_data)
        while 1:
            try:
                eachline = source_cur.fetchone()
            except:
                continue
            if eachline is None:
                None
            else:
                

                None
        None
    else:
        log.info("Can not find table : %s primary key...", tablename)
        log.info("Script will use all cols to match the row")
        None


getInsertrecord('SCOTT_10.45.15.201', 'impala_10.45.59.160', 'test_111')
# getPKColumn('SCOTT_10.45.15.201')
