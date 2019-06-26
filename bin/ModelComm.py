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
import RunSQL
import VariableUtil
from datetime import datetime

name = os.path.basename(__file__)
log = LogUtil.Logger(name)


# 加载结果
def file(path, msg, model='a', filename='compare_tmp'):
    filename = path + os.sep + '{filename}.txt'.format(filename=filename)
    with open(filename, model, encoding='utf-8') as f:
        f.write(msg)
    f.close()


# 比较
def commpare(path, type, dict={}):
    a = dict['SCOTT_10.45.15.201']
    b = dict['SCOTT_10.45.15.205']
    if len(list(set(b).difference(set(a)))) == 0 and len(list(set(a).difference(set(b)))) == 0:
        log.info('Consistent structure same')
        file(path, '%s比较结果	:   %s\n' % (type, '一致'))
        common = '||'.join(list(set(a).intersection(set(b))))
        if common:
            log.info('Elements %s in SCOTT_10.45.15.201 and SCOTT_10.45.15.205' %
                     ' '.join(list(set(a).intersection(set(b)))))
            file(path, '%s信息详情	:	%s\n' % (type, ''.join(list(set(a).intersection(set(b))))))
    else:
        log.info('Consistent structure different')
        file(path, '%s比较结果	:   %s\n' % (type, '不一致'))
        if len(list(set(b).difference(set(a)))) == 0 and len(list(set(a).difference(set(b)))) != 0:
            file(path, '共同%s详情	:   %s\n' % (type, ''.join(list(set(a).intersection(set(b))))))
        else:
            file(path, '共同%s详情	:   %s\n' % (type, ''.join(list(set(b).intersection(set(a))))))
        if len(list(set(a) - set(b))) > 0:
            log.error('Elements %s in SCOTT_10.45.15.205 but not in SCOTT_10.45.15.201' %
                      '||'.join(list(set(a) - set(b))))
            file(path, '比较库的%s	:	%s\n' % (type, ''.join(list(set(a).difference(set(b))))))
        if len(list(set(b) - set(a))) > 0:
            log.error('Elements %s in SCOTT_10.45.15.201 but not in SCOTT_10.45.15.205' %
                      '||'.join(list(set(a) - set(b))))
            file(path, '被比较库%s	:	%s\n' % (type, ''.join(list(set(b).difference(set(a))))))


# 字段和字段类型比较
def columnComm(tabname, path):
    log.info('Start to check table column name and type ..')
    dbrange = ['SCOTT_10.45.15.201', 'SCOTT_10.45.15.205']  # 后期改成入参
    dict = {}
    for i in dbrange:
        dbType, conn = DBConnect.getConnect(i)
        # 暂时只处理ORACLE
        if dbType == 'ORACLE':
            log.info('DB type is ORACLE, Start get columns...')
            sql = XmlUtil.dbExeSQL('COLUMN')
            sqlformat = sql.format(tabname=tabname)
            result = RunSQL.runSQL(conn, sqlformat)
            # 数据处理
            array = []
            for j in result:
                array.append(j[0])
            dict[i] = array
    # return dict
    commpare(path, '字段', dict)


# 约束比较
def constComm(tabname, path):
    log.info('Start to check table constraint ..')
    dbrange = ['SCOTT_10.45.15.201', 'SCOTT_10.45.15.205']  # 后期改成入参
    dict = {}
    for i in dbrange:
        dbType, conn = DBConnect.getConnect(i)
        # 暂时只处理ORACLE
        if dbType == 'ORACLE':
            log.info('DB type is ORACLE, Start get constraints...')
            sql = XmlUtil.dbExeSQL('CONSTRAINT')
            sqlformat = sql.format(tabname=tabname)
            result = RunSQL.runSQL(conn, sqlformat)
            # 数据处理
            array = []
            for j in result:
                detail = j[0] + '@' + j[1]
                array.append(detail)
            dict[i] = array
    # return dict
    commpare(path, '约束', dict)


# 索引比较
def idxComm(tabname, path):
    log.info('Start to check table index ..')
    dbrange = ['SCOTT_10.45.15.201', 'SCOTT_10.45.15.205']  # 后期改成入参
    dict = {}
    for i in dbrange:
        dbType, conn = DBConnect.getConnect(i)
        # 暂时只处理ORACLE
        if dbType == 'ORACLE':
            log.info('DB type is ORACLE, Start get indexes...')
            sql = XmlUtil.dbExeSQL('INDEX')
            sqlformat = sql.format(tabname=tabname)
            result = RunSQL.runSQL(conn, sqlformat)
            # 数据处理
            array = []
            for j in result:
                detail = j[0] + '@' + j[1]
                array.append(detail)
            dict[i] = array
    # return dict
    commpare(path, '索引', dict)


if __name__ == '__main__':
    TMP_PATH = VariableUtil.TMP_PATH
    file(VariableUtil.RLT_PATH, '任务比对环境	:	比较库-SCOTT_10.45.15.201&被比较库-SCOTT_10.45.15.205\n', 'w', 'compare')
    data = []
    for line in open(VariableUtil.CONF_PATH + os.sep + 'tabname.lst', 'r'):
        tabname = line.strip('\n')
        starttime = datetime.now()
        file(TMP_PATH, '任务比对对象	:	%s\n' % tabname, 'w')
        file(TMP_PATH, '任务启动时刻	:	%s\n' % starttime)
        file(TMP_PATH, '任务结束时刻	:	%s\n' % starttime)
        file(TMP_PATH, '任务总计耗时	:	%s\n' % starttime)
        columnComm(tabname, TMP_PATH)
        constComm(tabname, TMP_PATH)
        idxComm(tabname, TMP_PATH)
        file(TMP_PATH, '\n')
        endtime = datetime.now()
        ss = (endtime - starttime).seconds

        # 循环读取旧文件
        ff = open(TMP_PATH + os.sep + 'compare_tmp.txt', 'r', encoding='utf-8')
        for line in ff.readlines():
            # 进行判断
            if '任务结束时刻	:	%s\n' % starttime in line:
                line = line.replace('任务结束时刻	:	%s' % starttime, '任务结束时刻	:	%s' % endtime)
            elif '任务总计耗时	:	%s\n' % starttime in line:
                line = line.replace('任务总计耗时	:	%s' % starttime, '任务总计耗时	:	%s' % ss)
            file(VariableUtil.RLT_PATH, line, 'a', 'compare')

        ff.close()
