#!/usr/bin/env python
# -*- coding: utf-8 -*-

import DBConnect
import LogUtil
import os
import datetime

name = os.path.basename(__file__)
log = LogUtil.Logger(name)

source_dbType, source_conn = DBConnect.getConnect("sourceDB")
source_cur = source_conn.cursor()
target_dbType, target_conn = DBConnect.getConnect("targetDB")
target_cur = target_conn.cursor()

#  todo 表名先写死 后改成读配置文件
tableNames = ['STUDENT']

#  获取表对应的字段
columns_sql = '''select utc.table_name,listagg(utc.column_name,',') within group (order by utc.column_id) as columns 
from user_tab_cols utc
where utc.table_name in ('{tableNames}')
group by utc.table_name'''.format(tableNames="','".join(tableNames))
columns_results = source_cur.execute(columns_sql).fetchall()
#  获取表对应的主键
pk_sql = '''select b.table_name, listagg(b.column_name, ',') within group (order by b.position) as pkcol
from user_constraints a, user_cons_columns b
where a.table_name = b.table_name and a.constraint_type = 'P' and a.status = 'ENABLED' and a.constraint_name = b.constraint_name
and a.table_name in ('{tableNames}')
group by b.table_name'''.format(tableNames="','".join(tableNames))
pk_results = source_cur.execute(pk_sql).fetchall()

for columns_result in columns_results:
    table_name = columns_result[0]
    columns = columns_result[1]
    pk = ''
    for pk_result in pk_results:
        if table_name == pk_result[0]:
            pk = pk_result[1]
            break
    if pk == '':
        log.info('Table %s did not find primary key' % table_name)
    else:
        columns_list = columns.split(',')
        pk_list = pk.split(',')
        pk_position_list = []
        for pk_value in pk_list:
            for i in range(0, len(columns_list)):
                if pk_value == columns_list[i]:
                    pk_position_list.append(i)
                    break

        #   正向比对
        source_sql = 'select {columns} from {tableName}'.format(columns=columns, tableName=table_name)
        source_cur.execute(source_sql)
        while True:
            #   正向比对结果
            insert_update_list = []
            insert_list = []
            update_list = []
            source_list = source_cur.fetchmany(999)
            pk_str_list = [""] * len(pk_position_list)
            for source in source_list:
                for j in range(0, len(pk_position_list)):
                    pk_str_list[j] = pk_str_list[j] + "','" + str(source[pk_position_list[j]])
            condition_sql = ""
            for n in range(0, len(pk_list)):
                condition_sql = condition_sql + pk_list[n] + " in ('" + pk_str_list[n][3:] + "')" + " and "
            condition_sql = condition_sql[:-4]

            target_sql = "select {columns} from {tableName} where {condition}".format(columns=columns,
                                                                                      tableName=table_name,
                                                                                      condition=condition_sql)
            target_cur.execute(target_sql)
            target_list = target_cur.fetchall()
            insert_update_list = list(set(source_list) - set(target_list))

            target_pk_str = ','
            for target in target_list:
                for i in range(0, len(pk_position_list)):
                    target_pk_str = target_pk_str + target[pk_position_list[i]] + '-'
                target_pk_str = target_pk_str[:-1] + ','

            for insert_update in insert_update_list:
                pk_str = ''
                for i in range(0, len(pk_position_list)):
                    pk_str = pk_str + insert_update[pk_position_list[i]] + '-'
                if ',' + pk_str[:-1] + ',' in target_pk_str:
                    update_list.append(insert_update)
                else:
                    insert_list.append(insert_update)

            print('current time %s insert data:%s' % (
                (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S"), insert_list))
            print('current time %s update data:%s' % (
                (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S"), update_list))

            #   最后一波数据跳出循环
            if len(source_list) < 999:
                break

        #   反向比对 只查主键
        pk_sql = 'select {columns} from {tableName}'.format(columns=pk, tableName=table_name)
        target_cur.execute(pk_sql)
        while True:
            delete_list = []
            re_source_list = target_cur.fetchmany(999)
            pk_str_list = [""] * len(pk_position_list)
            for re_source in re_source_list:
                for j in range(0, len(pk_position_list)):
                    pk_str_list[j] = pk_str_list[j] + "','" + str(re_source[j])
            condition_sql = ""
            for n in range(0, len(pk_list)):
                condition_sql = condition_sql + pk_list[n] + " in ('" + pk_str_list[n][3:] + "')" + " and "
            condition_sql = condition_sql[:-4]
            target_sql = "select {columns} from {tableName} where {condition}".format(columns=pk,
                                                                                      tableName=table_name,
                                                                                      condition=condition_sql)
            source_cur.execute(target_sql)
            re_target_list = source_cur.fetchall()
            delete_list = list(set(re_source_list) - set(re_target_list))

            print('current time %s delete data:%s' % (
            (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S"), delete_list))
            #   最后一波数据跳出循环
            if len(re_source_list) < 999:
                break
