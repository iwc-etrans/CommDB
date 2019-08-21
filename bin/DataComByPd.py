#!/usr/bin/env python
# -*- coding: utf-8 -*-

import DBConnect
import LogUtil
import os
import RunSQL
import pandas as pd
from multiprocessing import Pool
import datetime
import VariableUtil
import sys

name = os.path.basename(__file__)
log = LogUtil.Logger(name)
#   数据比较阀值
dataCount = {'source': 50000, 'target': 50000}


def get_pk_column(auth):
    db_type, conn = DBConnect.getConnect(auth)
    # 暂时只处理ORACLE
    dir_pk_cols = {}
    if db_type == 'ORACLE':
        log.info('DB type is ORACLE, Start get all primary key...')
        sql = "select b.TABLE_NAME,listagg(b.COLUMN_NAME, ',') within group (order by b.POSITION) as pkcol from USER_CONSTRAINTS a,USER_CONS_COLUMNS b where a.TABLE_NAME = b.TABLE_NAME and a.CONSTRAINT_TYPE = \'P\' and a.STATUS = \'ENABLED\' and a.constraint_name = b.constraint_name group by b.table_name"
        # 获取所有状态为enabled 的主键表名及主键字段
        result = RunSQL.runSQL(conn, sql)
        for arr in result:
            dir_pk_cols[arr[0]] = arr[1]
        log.debug("All primary key is %s", dir_pk_cols)
        return dir_pk_cols


def get_all_column(auth):
    db_type, conn = DBConnect.getConnect(auth)
    # 暂时只处理ORACLE
    dir_all_cols = {}
    if db_type == 'ORACLE':
        log.info('DB type is ORACLE, Start get all primary key...')
        sql = "select utc.table_name,listagg(utc.column_name,',') within group (order by utc.column_id) as columns from user_tab_cols utc group by utc.table_name"
        # 获取所有状态为enabled 的主键表名及主键字段
        result = RunSQL.runSQL(conn, sql)
        for arr in result:
            dir_all_cols[arr[0]] = arr[1]
        log.debug("All primary key is %s", dir_all_cols)
        return dir_all_cols


def col_type_change(db_type, col_list):
    condition_sql = ''
    col_list = col_list.split(',')
    if db_type == 'ORACLE':
        for pk_value in col_list:
            condition_sql = condition_sql + 'to_char(' + pk_value + '),'
    elif db_type == 'IMPALA_KUDU':
        for pk_value in col_list:
            condition_sql = condition_sql + 'cast(' + pk_value + ' as string),'
    condition_sql = condition_sql[:-1]
    return condition_sql


def compare_data(table_name, regexp):
    source_db_type, source_conn = DBConnect.getConnect('sourceDB')
    target_db_type, target_conn = DBConnect.getConnect('targetDB')
    pk_cols_arr = get_pk_column('sourceDB')
    all_cols_arr = get_all_column('sourceDB')
    table_name = str.upper(table_name)
    if table_name in pk_cols_arr.keys():
        pk_cols = pk_cols_arr[table_name]
        log.info("Find table : %s primary key cols is %s", table_name, pk_cols)
        source_cur = source_conn.cursor()
        target_cur = target_conn.cursor()
        all_cols = all_cols_arr[table_name]
        all_cols_list = all_cols.split(',')
        pk_cols_list = pk_cols.split(',')
        log.info("Find table : %s all cols is %s", table_name, all_cols)
        source_condition_sql = col_type_change(source_db_type, pk_cols)
        target_condition_sql = col_type_change(target_db_type, pk_cols)
        regexp_condition = ''
        if regexp != 0:
            regexp_condition = "where REGEXP_LIKE(" + pk_cols_list[0] + ",'" + regexp + "')"
        sql_get_source_data = "select %(cols)s from %(table)s %(reg_condition)s order by %(condition)s" % {
            'cols': all_cols, 'table': table_name, 'reg_condition': regexp_condition, 'condition': source_condition_sql}
        sql_get_target_data = "select %(cols)s from %(table)s %(reg_condition)s order by %(condition)s" % {
            'cols': all_cols, 'table': table_name, 'reg_condition': regexp_condition, 'condition': target_condition_sql}

        source_cur.execute(sql_get_source_data)
        target_cur.execute(sql_get_target_data)

        source_left_frame = pd.DataFrame(columns=all_cols_list)
        target_left_frame = pd.DataFrame(columns=all_cols_list)
        update_left_frame = pd.DataFrame(columns=all_cols_list)
        cycle_count = 0
        while True:
            source_results = source_cur.fetchmany(dataCount['source'])
            cycle_count = cycle_count + 1
            log.info("current batch is " + str(cycle_count))
            if not source_results and cycle_count < 1:
                print("------------------insert_data---------")
                print(source_left_frame)
                print("------------------delete_data---------")
                print(target_left_frame)
                print("------------------update_data---------")
                print(update_left_frame)
                print("-----------" + str(os.getpid()) + " over-----------")
                break
            else:
                source_frame = pd.DataFrame(list(source_results), columns=all_cols_list)
                source_left_frame = source_left_frame.append(source_frame)
                source_left_pk_frame = source_left_frame[pk_cols_list]
                while True:
                    target_results = target_cur.fetchmany(dataCount['target'])
                    if target_results:
                        target_frame = pd.DataFrame(list(target_results), columns=all_cols_list)
                        target_left_frame = target_left_frame.append(target_frame)
                    #   内层循环结束标记
                    else:
                        cycle_count = -1

                    target_left_pk_frame = target_left_frame[pk_cols_list]
                    result_all_col = pd.merge(source_left_frame, target_left_frame, how='outer', on=all_cols_list,
                                              indicator=True)
                    result_pk_col = pd.merge(source_left_pk_frame, target_left_pk_frame, how='outer',
                                             on=pk_cols_list,
                                             indicator=True)
                    diff_data = pd.merge(result_all_col, result_pk_col, how='inner', on=pk_cols_list)
                    source_left_frame = diff_data[
                        (diff_data._merge_x == 'left_only') & (
                                diff_data._merge_y != 'both')][all_cols_list]
                    target_left_frame = diff_data[
                        (diff_data._merge_x == 'right_only') & (
                                diff_data._merge_y != 'both')][all_cols_list]
                    update_left_frame = update_left_frame.append(diff_data[
                                                                     (diff_data._merge_x == 'left_only') & (
                                                                             diff_data._merge_y == 'both')][
                                                                     all_cols_list])
                    break


def compare_data_multiple(table_name, flag):
    if flag == 0:
        compare_data(table_name, flag)
    else:
        thread_num_total = int(flag)
        log.info('Table:' + str(table_name) + '--------------------------' + datetime.datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S'))
        thread_num_current = thread_num_total
        pool = Pool(processes=thread_num_total)
        log.info("start!!!!  threadTotal：" + str(thread_num_total))

        step = 100 // thread_num_total
        regex_count = 0

        while thread_num_current > 0:
            regex_end = regex_count + step - 1
            if regex_end >= 9:
                regex_end = 9
            regexp = '[' + str(regex_count) + '-' + str(regex_end) + ']$'
            thread_num_current = thread_num_current - 1
            regex_count = regex_end
            log.info(
                'Process:' + str(thread_num_current) + '--------------------------' + datetime.datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'))
            pool.apply_async(func=compare_data, args=(table_name, regexp))
            log.info("start process!!!!  ProcessNum：" + str(os.getpid()))
        pool.close()
        pool.join()


if __name__ == '__main__':
    for line in open(VariableUtil.CONF_PATH + os.sep + 'tabname.lst', 'r'):
        tableName = line.strip('\n')
        progressNum = sys.argv[1]
        compare_data_multiple(tableName, progressNum)
