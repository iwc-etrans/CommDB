#!/usr/bin/env python
# -*- coding: utf-8 -*-

import DBConnect
import LogUtil
import os
import RunSQL
import pandas as pd

name = os.path.basename(__file__)
log = LogUtil.Logger(name)
#   数据比较阀值
dataCount = {'source': 50000, 'target': 50000, 'persent': 0.1}


def getPKColumn(auth):
    dbType, conn = DBConnect.getConnect(auth)
    # 暂时只处理ORACLE
    dir_pkcols = {}
    if dbType == 'ORACLE':
        log.info('DB type is ORACLE, Start get all primary key...')
        sql = "select b.TABLE_NAME,listagg(b.COLUMN_NAME, ',') within group (order by b.POSITION) as pkcol from USER_CONSTRAINTS a,USER_CONS_COLUMNS b where a.TABLE_NAME = b.TABLE_NAME and a.CONSTRAINT_TYPE = \'P\' and a.STATUS = \'ENABLED\' and a.constraint_name = b.constraint_name group by b.table_name"
        # 获取所有状态为enabled 的主键表名及主键字段
        result = RunSQL.runSQL(conn, sql)
        for arr in result:
            dir_pkcols[arr[0]] = arr[1]
        log.debug("All primary key is %s", dir_pkcols)
        return dir_pkcols


def getAllColumn(auth):
    dbType, conn = DBConnect.getConnect(auth)
    # 暂时只处理ORACLE
    dir_allcols = {}
    if dbType == 'ORACLE':
        log.info('DB type is ORACLE, Start get all primary key...')
        sql = "select utc.table_name,listagg(utc.column_name,',') within group (order by utc.column_id) as columns from user_tab_cols utc group by utc.table_name"
        # 获取所有状态为enabled 的主键表名及主键字段
        result = RunSQL.runSQL(conn, sql)
        for arr in result:
            dir_allcols[arr[0]] = arr[1]
        log.debug("All primary key is %s", dir_allcols)
        return dir_allcols


def colTypeChange(DBtype, col_list):
    condition_sql = ''
    col_list = col_list.split(',')
    # print(col_list)
    if DBtype == 'ORACLE':
        for pk_value in col_list:
            condition_sql = condition_sql + 'to_char(' + pk_value + '),'
    elif DBtype == 'IMPALA_KUDU':
        for pk_value in col_list:
            condition_sql = condition_sql + 'cast(' + pk_value + ' as string),'
    condition_sql = condition_sql[:-1]
    return condition_sql


def main(sourceauth, targetauth, tablename):
    source_dbType, source_conn = DBConnect.getConnect(sourceauth)
    target_dbType, target_conn = DBConnect.getConnect(targetauth)
    pkcols_arr = getPKColumn(sourceauth)
    allcols_arr = getAllColumn(sourceauth)
    tablename = str.upper(tablename)
    if tablename in pkcols_arr.keys():
        pk_cols = pkcols_arr[tablename]
        log.info("Find table : %s primary key cols is %s", tablename, pk_cols)
        source_cur = source_conn.cursor()
        target_cur = target_conn.cursor()
        # print(allcols_arr)
        all_cols = allcols_arr[tablename]
        all_cols_list = all_cols.split(',')
        pk_cols_list = pk_cols.split(',')
        log.info("Find table : %s all cols is %s", tablename, all_cols)
        source_condition_sql = colTypeChange(source_dbType, pk_cols)
        target_condition_sql = colTypeChange(target_dbType, pk_cols)
        # print(source_condition_sql)
        sql_get_source_data = "select %(cols)s from %(table)s order by %(condition)s" % {'cols': all_cols,
                                                                                         'table': tablename,
                                                                                         'condition': source_condition_sql}
        source_cur.execute(sql_get_source_data)
        sql_get_target_data = "select %(cols)s from %(table)s order by %(condition)s" % {'cols': all_cols,
                                                                                         'table': tablename,
                                                                                         'condition': target_condition_sql}
        source_cur.execute(sql_get_source_data)
        target_cur.execute(sql_get_target_data)

        source_left_fream = pd.DataFrame(columns=all_cols_list)
        source_left_pk_fream = pd.DataFrame(columns=pk_cols_list)
        target_left_fream = pd.DataFrame(columns=all_cols_list)
        target_left_pk_fream = pd.DataFrame(columns=pk_cols_list)
        update_left_fream = pd.DataFrame(columns=all_cols_list)
        cycle_count = 0
        while True:
            source_results = source_cur.fetchmany(dataCount['source'])
            cycle_count = cycle_count + 1
            print(cycle_count)
            if not source_results:
                print("------------------finally-------------")
                print("------------------insert_data---------")
                print(source_left_fream)
                print("------------------delete_data---------")
                print(target_left_fream)
                print("------------------update_data---------")
                print(update_left_fream)
                break
            else:
                source_fream = pd.DataFrame(list(source_results), columns=all_cols_list)
                source_pk_fream = source_fream[pk_cols_list]
                source_left_fream = source_left_fream.append(source_fream)
                source_left_pk_fream = source_left_pk_fream.append(source_pk_fream)
                while True:
                    target_results = target_cur.fetchmany(dataCount['target'])
                    if not target_results:
                        result_all_col = pd.merge(source_left_fream, target_left_fream, how='outer', on=all_cols_list,
                                                  indicator=True)
                        result_pk_col = pd.merge(source_left_pk_fream, target_left_pk_fream, how='outer',
                                                 on=pk_cols_list,
                                                 indicator=True)
                        diff_data = pd.merge(result_all_col, result_pk_col, how='inner', on=pk_cols_list)
                        source_left_fream = diff_data[
                            (diff_data._merge_x == 'left_only') & (
                                    diff_data._merge_y != 'both')][all_cols_list]
                        target_left_fream = diff_data[
                            (diff_data._merge_x == 'right_only') & (
                                    diff_data._merge_y != 'both')][all_cols_list]
                        update_left_fream = update_left_fream.append(diff_data[
                                                                         (diff_data._merge_x == 'left_only') & (
                                                                                 diff_data._merge_y == 'both')][
                                                                         all_cols_list])
                        break
                    else:
                        target_fream = pd.DataFrame(list(target_results), columns=all_cols_list)
                        target_pk_fream = target_fream[pk_cols_list]
                        target_left_fream = target_left_fream.append(target_fream)
                        target_left_pk_fream = target_left_pk_fream.append(target_pk_fream)
                        result_all_col = pd.merge(source_left_fream, target_left_fream, how='outer', on=all_cols_list,
                                                  indicator=True)
                        result_pk_col = pd.merge(source_left_pk_fream, target_left_pk_fream, how='outer',
                                                 on=pk_cols_list,
                                                 indicator=True)
                        diff_data = pd.merge(result_all_col, result_pk_col, how='inner', on=pk_cols_list)
                        source_left_fream = diff_data[
                            (diff_data._merge_x == 'left_only') & (
                                    diff_data._merge_y != 'both')][all_cols_list]
                        target_left_fream = diff_data[
                            (diff_data._merge_x == 'right_only') & (
                                    diff_data._merge_y != 'both')][all_cols_list]
                        update_left_fream = update_left_fream.append(diff_data[
                                                                         (diff_data._merge_x == 'left_only') & (
                                                                                 diff_data._merge_y == 'both')][
                                                                         all_cols_list])
                        break


if __name__ == '__main__':
    main('sourceDB', 'targetDB', 'sc')
