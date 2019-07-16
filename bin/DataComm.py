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

name = os.path.basename(__file__)
log = LogUtil.Logger(name)


# 加载结果
def file(path, msg, model='a', filename='DataCompare_tmp'):
    filename = path + os.sep + '{filename}.txt'.format(filename=filename)
    with open(filename, model, encoding='utf-8') as f:
        f.write(msg)
    f.close()


# 比较
def commpare(path, type, compare=[], becompare=[]):
    a = compare
    b = becompare[0]
    val_com = becompare[1]
    if len(list(set(b).difference(set(a)))) == 0 and len(list(set(a).difference(set(b)))) == 0:
        log.info('Table attribute same')
        file(path, '数据比较结果	:   一致\n')
        snum = len(list(set(a).intersection(set(b))))
        file(path, '数据相同总数	:	%s\n' % snum)
    else:
        log.info('Table attribute different')
        file(path, '数据比较结果	:   不一致\n')
        if len(list(set(b).difference(set(a)))) == 0 and len(list(set(a).difference(set(b)))) != 0:
            file(path, '数据相同总数	:   %s\n' % len(list(set(a).intersection(set(b)))))
        elif len(list(set(a).difference(set(b)))) == 0 and len(list(set(b).difference(set(a)))) != 0:
            file(path, '数据相同总数	:   %s\n' % len(list(set(b).intersection(set(a)))))
        if len(list(set(a).difference(set(b)))) > 0:
            # log.error('Attribute %s in SCOTT_10.45.15.205 but not in SCOTT_10.45.15.201' %
            #           ''.join(str(list(set(a).difference(set(b))))))
            # file(path, '比较库的详情	:	%s\n' % ''.join(str(list(set(a).difference(set(b))))))
            log.error('Attribute %s in SCOTT_10.45.15.201 but not in SCOTT_10.45.15.205' %
                      val_com)
            file(path, '仅比较库存在	:	%s\n' % val_com)


# 获取主键
def getPKColumn(auth, tabname):
    dbType, conn = DBConnect.getConnect(auth)
    # 暂时只处理ORACLE
    if dbType == 'ORACLE':
        log.info('DB type is ORACLE, Start get constraints...')
        sql = XmlUtil.dbExeSQL('CONSTRAINT')
        sqlformat = sql.format(tabname=tabname)
        result = RunSQL.runSQL(conn, sqlformat)
        # 数据处理
        array = []
        for j in result:
            detail = j[1].split(',')
            for k in detail:
                if 'P_' in k:
                    array.append(k.split('_')[1])
        return array


# 根据主键创建比对临时表
def creTmpTab(auth, tabname, array=[]):
    dbType, conn = DBConnect.getConnect(auth)
    colarray = []
    # 暂时只处理ORACLE
    if dbType == 'ORACLE':
        log.info('DB type is ORACLE, Start get columns')
        sql = XmlUtil.dbExeSQL('COLUMN')
        sqlformat = sql.format(tabname=tabname)
        result = RunSQL.runSQL(conn, sqlformat)
        # 数据处理
        for i in array:
            for j in result:
                if i in j[0]:
                    colarray.append(j[0].split(',')[0])
        crecol = ','.join(colarray)
        tab = 'COMM_TMP'
        sql = 'CREATE TABLE %s (%s)' % (tab, crecol)
        result_cnt = RunSQL.runSQL(DBConnect.getConnect(auth)[1],
                                   "select count(1) from user_tables where table_name ='%s'" % tab)
        if result_cnt[0][0] == 0:
            log.info('%s not exists' % tab)
            RunSQL.ddlSQL(DBConnect.getConnect(auth)[1], sql)
            log.info('Create table successful')
        else:
            RunSQL.ddlSQL(DBConnect.getConnect(auth)[1], 'delete from %s' % tab)
            log.info('Delete table successful')
            RunSQL.ddlSQL(DBConnect.getConnect(auth)[1], 'drop table %s' % tab)
            log.info('Drop table successful')
            RunSQL.ddlSQL(DBConnect.getConnect(auth)[1], sql)
            log.info('Create table successful')


# 比较库数据写入比对中间表
def getCommDB(srcauth, trgauth, tabname, tmptab, array=[]):
    dbType, conn = DBConnect.getConnect(srcauth)
    # 暂时只处理ORACLE
    if dbType == 'ORACLE':
        # 获取数据
        col = ','.join(array)
        cur = conn.cursor()
        cur.execute('select %s from %s' % (col, tabname))
        data = []
        insert = {}
        # :字段
        value = []
        c = []
        commparelist = []
        for j in array:
            value.append(':%s' % j)
        while True:
            result = cur.fetchone()
            if result is None:
                sql = 'insert into %s(%s) values(%s)' % (tmptab, col, ','.join(value))
                # print(sql)
                RunSQL.dmlManySQL(DBConnect.getConnect(trgauth)[1], sql, data)
                break
            else:
                for i in range(0, len(array)):
                    c.append("{colname} = '{value}'".format(colname=array[i], value=result[i]))
                    insert[array[i]] = result[i]
                cc = ' and '.join(c)
                commpare = RunSQL.runSQL(DBConnect.getConnect(srcauth)[1], 'select * from %s where %s' % (tabname, cc))
                c = []
                commparelist.append(commpare[0])
                data.append(insert)
                # print(data)
                # 初始化
                insert = {}
                if len(data) == 1000:
                    sql = 'insert into %s(%s) values(%s)' % (tmptab, col, ','.join(value))
                    RunSQL.dmlManySQL(DBConnect.getConnect(trgauth)[1], sql, data)
                    data = []
        cur.close()
        conn.close()
        return commparelist


# 比对中间和目标表结果比对
def CommTab(srcauth, trgauth, tabname, tmptab, array=[]):
    data = []
    for i in array:
        c = 's.%s = t.%s' % (i, i)
        data.append(c)
    condition = ' and '.join(data)
    sql = XmlUtil.dbExeSQL('TMP')
    sqlformat = sql.format(srcname=tabname, trgname=tmptab, condition=condition)
    flag = RunSQL.runSQL(DBConnect.getConnect(trgauth)[1], sqlformat)
    # print (sqlformat)
    if len(flag) == 0:
        log.info('Compare Current compare data PK structure same in becompare')
        log.info('Start check table attribute')
        sql = XmlUtil.dbExeSQL('TRG_EXISTS')
        sqlformat = sql.format(srcname=tabname, trgname=tmptab, condition=condition)
        # print(sqlformat)
        becompare = RunSQL.runSQL(DBConnect.getConnect(trgauth)[1], sqlformat)
        # sql = XmlUtil.dbExeSQL('TRG_NOT_EXISTS')
        # sqlformat = sql.format(srcname=srcname, trgname=trgname, condition=condition)
        # print(sqlformat)
        # becompare2 = RunSQL.runSQL(DBConnect.getConnect(trgauth)[1], sqlformat)
        return [becompare, '']
    else:
        log.info('Compare Current compare data PK structure different in becompare ,%s PK more then %s' % (
            tmptab, tabname))
        # print(array, flag)
        sql = XmlUtil.dbExeSQL('TRG_EXISTS')
        sqlformat = sql.format(srcname=tabname, trgname=tmptab, condition=condition)
        # print(sqlformat)
        becompare = RunSQL.runSQL(DBConnect.getConnect(trgauth)[1], sqlformat)
        cond = []
        for i in range(0, len(array)):
            c = "{key} = '{value}'".format(key=array[i], value=flag[0][i])
            cond.append(c)
        conds = ' and '.join(cond)
        sql = 'select * from {tabname} where {condition}'.format(tabname=tabname, condition=conds)
        val_com = RunSQL.runSQL(DBConnect.getConnect(srcauth)[1], sql)
        return [becompare, val_com]


# 小数据量比对，海量数据比对后期改造，整合getCommDB , CommTab
if __name__ == '__main__':
    TMP_PATH = VariableUtil.TMP_PATH
    file(VariableUtil.RLT_PATH, '任务比对环境	:	比较库-SCOTT_10.45.15.201&被比较库-SCOTT_10.45.15.205\n', 'w', 'DataCompare')
    file(VariableUtil.RLT_PATH, '任务比对模式	:	数据比对\n', 'a', 'DataCompare')
    data = []
    for line in open(VariableUtil.CONF_PATH + os.sep + 'tabname.lst', 'r'):
        tabname = line.strip('\n')
        starttime = datetime.now()
        file(TMP_PATH, '任务比对对象	:	%s\n' % tabname, 'w')
        file(TMP_PATH, '任务启动时刻	:	%s\n' % starttime)
        file(TMP_PATH, '任务结束时刻	:	%s\n' % starttime)
        file(TMP_PATH, '任务总计耗时	:	%s\n' % starttime)
        pk = getPKColumn('SCOTT_10.45.15.201', tabname)
        creTmpTab('SCOTT_10.45.15.205', tabname, pk)
        a = getCommDB('SCOTT_10.45.15.201', 'SCOTT_10.45.15.205', tabname, 'COMM_TMP', pk)
        b = CommTab('SCOTT_10.45.15.201', 'SCOTT_10.45.15.205', tabname, 'COMM_TMP', pk)
        print(b, "-------------------")
        commpare(TMP_PATH, type, a, b)
        file(TMP_PATH, '\n')
        endtime = datetime.now()
        ss = (endtime - starttime).seconds

        # 循环读取旧文件
        ff = open(TMP_PATH + os.sep + 'DataCompare_tmp.txt', 'r', encoding='utf-8')
        for line in ff.readlines():
            # 进行判断
            if '任务结束时刻	:	%s\n' % starttime in line:
                line = line.replace('任务结束时刻	:	%s' % starttime, '任务结束时刻	:	%s' % endtime)
            elif '任务总计耗时	:	%s\n' % starttime in line:
                line = line.replace('任务总计耗时	:	%s' % starttime, '任务总计耗时	:	%s' % ss)
            file(VariableUtil.RLT_PATH, line, 'a', 'DataCompare')
        ff.close()
