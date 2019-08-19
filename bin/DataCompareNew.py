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

#  todo 表名先写死 后改成读配置文件
tableNames = ['SC']

#   数据比较阀值
dataCount = [20000, 30000, 2000]

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
        #   主键在字段中对应的位置
        pk_position_list = []
        for pk_value in pk_list:
            for i in range(0, len(columns_list)):
                if pk_value == columns_list[i]:
                    pk_position_list.append(i)
                    break

        oracle_condition_sql = 'order by '
        imp_condition_sql = 'order by '
        pk_list = pk.split(',')
        for pk_value in pk_list:
            oracle_condition_sql = oracle_condition_sql + 'to_char(' + pk_value + '),'
            imp_condition_sql = imp_condition_sql + 'cast(' + pk_value + ' as string),'
        oracle_condition_sql = oracle_condition_sql[:-1]
        imp_condition_sql = imp_condition_sql[:-1]

        result_sql = 'select {columns} from {tableName} {condition}'.format(tableName=table_name, columns='{columns}',
                                                                            condition='{condition}')

        source_columns_cur = source_conn.cursor()
        source_columns_cur.execute(result_sql.format(columns=columns, condition=oracle_condition_sql))
        source_pk_cur = source_conn.cursor()
        source_pk_cur.execute(result_sql.format(columns=pk, condition=oracle_condition_sql))

        target_columns_cur = target_conn.cursor()
        target_columns_cur.execute(result_sql.format(columns=columns, condition=imp_condition_sql))
        target_pk_cur = target_conn.cursor()
        target_pk_cur.execute(result_sql.format(columns=pk, condition=imp_condition_sql))

        #   每次循环遗留数据
        source_col_left = []
        source_pk_left = []
        target_col_left = []
        target_pk_left = []

        insert_results = []
        update_results = []

        while True:

            source_columns_results = source_columns_cur.fetchmany(dataCount[0])
            source_pk_results = source_pk_cur.fetchmany(dataCount[0])
            if len(target_pk_left) > 0:
                i_u_col_results = list(set(source_columns_results) - set(target_col_left))
                i_pk_results = list(set(source_pk_results) - set(target_pk_left))
                for i_u_col_result in i_u_col_results:
                    i_u_pk = []
                    for p in pk_position_list:
                        i_u_pk.append(i_u_col_result[p])
                    i_u_pk_tuple = tuple(i_u_pk)
                    if i_u_pk_tuple in i_pk_results:
                        source_col_left.append(i_u_col_result)
                        source_pk_left.append(i_u_pk_tuple)
                    else:
                        update_results.append(i_u_col_result)
            else:
                source_col_left.extend(source_columns_results)
                source_pk_left.extend(source_pk_results)

            while True:
                target_columns_results = target_columns_cur.fetchmany(dataCount[1])
                target_pk_results = target_pk_cur.fetchmany(dataCount[1])

                #   反向
                delete_col_results = list(
                    set(target_columns_results + target_col_left) - set(source_columns_results))
                delete_pk_results = list(set(target_pk_results + target_pk_left) - set(source_pk_results))
                target_col_left = []
                target_pk_left = []
                for delete_col_result in delete_col_results:
                    delete_pk = []
                    for q in pk_position_list:
                        delete_pk.append(delete_col_result[q])
                    if tuple(delete_pk) in delete_pk_results:
                        target_col_left.append(delete_col_result)
                target_pk_left = delete_pk_results

                if len(target_pk_results) > 0:

                    #   正向
                    insert_update_col_results = list(set(source_col_left) - set(target_columns_results))
                    insert_pk_results = list(set(source_pk_left) - set(target_pk_results))
                    #   释放数据
                    source_col_left = []
                    source_pk_left = []
                    for insert_update_col_result in insert_update_col_results:
                        insert_update_pk = []
                        for p in pk_position_list:
                            insert_update_pk.append(insert_update_col_result[p])
                        insert_update_pk_tuple = tuple(insert_update_pk)
                        if insert_update_pk_tuple in insert_pk_results:
                            source_col_left.append(insert_update_col_result)
                            source_pk_left.append(insert_update_pk_tuple)
                        else:
                            update_results.append(insert_update_col_result)

                    #   源库数据到达阀值
                    if len(source_col_left) < dataCount[2]:
                        break
                if len(source_pk_results) < dataCount[0]:
                    break
            print('%s update result:%s' % ((datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S"), update_results))
            if len(source_pk_results) < dataCount[0]:
                break

        print('Table %s insert result:%s' % (table_name, insert_results))
        print('Table %s update result:%s' % (table_name, update_results))
        print('Table %s delete result:%s' % (table_name, target_col_left))
