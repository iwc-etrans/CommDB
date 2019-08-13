#!/usr/bin/env python
# -*- coding: utf-8 -*-

import DBConnect
import LogUtil
import os

name = os.path.basename(__file__)
log = LogUtil.Logger(name)


def compare(source_cursor, target_cursor, sql, pk_position):
    different_list=[]
    source_cursor.execute(sql)
    while True:
        source_list = source_cursor.fetchmany(9)
        pk_str_list = [""] * len(pk_position)
        for i in range(0, len(source_list)):
            for j in range(0, len(pk_position)):
                pk_str_list[j] = pk_str_list[j] + "','" + str(source_list[i][pk_position[j]])
        condition_sql = ""
        for n in range(0, len(pk_list)):
            condition_sql = condition_sql + pk_list[n] + " in ('" + pk_str_list[n][3:] + "')" + " and "
        condition_sql = condition_sql[:-4]

        target_sql = source_sql + " where {condition}".format(condition=condition_sql)
        target_cursor.execute(target_sql)
        target_list = target_cursor.fetchmany(9)
        different_list.append(list(set(source_list) - set(target_list)))
        #   最后一波数据跳出循环
        if len(source_list) < 999:
            break
    return different_list


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
        for pk in pk_list:
            for i in (0, len(columns_list)):
                if pk == columns_list[i]:
                    pk_position_list.append(i)
                    break

        source_sql = 'select {columns} from {tableName}'.format(columns=columns, tableName=table_name)

        #   正向比对
        insert_update_list=compare(source_cur,target_cur,source_sql,pk_position_list)
        #   反向比对
        delete_list=compare(target_cur,source_cur,source_sql,pk_position_list)

        print(insert_update_list)
        print(delete_list)




