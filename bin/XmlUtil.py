#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/6/13 8:47
# @Author  : Fcvane
# @Param   : 配置文件解析
# @File    : XmlAnalysis.py

import PasswdUtil
import os, sys
import LogUtil
import VariableUtil

try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree

name = os.path.basename(__file__)
log = LogUtil.Logger(name)

'''
解析数据库参数集目录为：../conf/dbParams
格式为：
<?xml version='1.0' encoding='utf-8'?>
<configuration>
    <auth db="SCOTT_10.45.15.201">
        <db type="oracle">
            <host>10.45.15.20</host>
            <port>1521</port>
            <sid>orcl</sid>
            <serverName></serverName>
            <userName>scott</userName>
            <passWord>xoiMDvbRNj8hS8f7QkVdwR5VTVnNHiLBXKEPE/pd2DyYxwW0gVL3VYiw6Mr6hv9+hF5TqD5/WGc4wuolwHup9Q==</passWord>
            <enable>Ture</enable>
        </db>
    </auth>
</configuration>
'''


def dbCFGInfo(auth):
    taskName = VariableUtil.DB_PATH + os.sep + 'db_commondb.xml'
    log.info('Start to analysis {taskName}'.format(taskName=taskName))
    result = {}
    try:
        tree = etree.parse(taskName)
        # 获得子元素
        elemlist = tree.findall('auth[@id="%s"]' % auth)
        # 遍历task所有子元素
        for elem in elemlist:
            array = {}
            for child in elem.getchildren():
                # print(child.tag, ":", child.text)
                if child.tag == "passWord":
                    array[child.tag] = PasswdUtil.decrypt(child.text)
                else:
                    array[child.tag] = child.text
            result[elem.attrib['id']] = array
            log.info('File {taskName} analysis sucessful '.format(taskName=taskName))
    except Exception as e:
        log.error('File {taskName} analysis failure '.format(taskName=taskName))
        sys.exit()
    # print(result)
    return result


'''
<?xml version='1.0' encoding='utf-8'?>
<configuration>
    <type name="COLUMN">
        <sql>"select column_name,
            case
            when data_type = 'NUMBER' and data_scale = 0 and
            data_precision is not null then
            column_name || ' ' || data_type || '(' || data_precision || '),'
            when data_type = 'NUMBER' and data_scale = 0 and
            data_precision is null then
            column_name || ' ' || data_type || ','
            when data_type = 'NUMBER' and data_scale > 0 then
            column_name || ' ' || data_type || '(' || data_precision || ',' ||
            data_scale || '),'
            when data_type = 'NUMBER' and data_scale is null then
            column_name || ' ' || data_type || ','
            when data_type = 'VARCHAR2' then
            column_name || ' ' || data_type || '(' || data_length || '),'
            when data_type = 'CHAR' then
            column_name || ' ' || data_type || '(' || data_length || '),'
            when data_type = 'FLOAT' then
            column_name || ' ' || data_type || '(' || data_precision || '),'
            else
            column_name || ' ' || data_type || ','
            end
            from user_tab_cols
            where table_name = '%s'
            order by column_id;"%tabname
        </sql>
    </type>
</configuration>
'''

# def queryExecSqlInfo(authId,taskName):
#     authDbId = authId.upper()
#     print (authDbId)
#     elem = []
#     result = []
#     print ('EXECSQL_CONFIG_PATH:'+EXECSQL_CONFIG_PATH)
#     files = os.listdir(EXECSQL_CONFIG_PATH)
#     for f in files:
#         #所以配置文件都规定为大写
#         if f == authDbId + '_SQL.CFG':
#             filePath = EXECSQL_CONFIG_PATH + os.sep + authDbId + '_SQL.CFG'
#             print filePath
#             tree = etree.parse(filePath)
#             root = tree.getroot()
#             elem = tree.find('./task[@name="%s"]' % taskName)
#             for child in elem.getchildren():
#              result += [child.text]
#             break
#     return result

if __name__ == '__main__':
    dbConfig = dbCFGInfo('SCOTT_10.45.15.201')
    print(dbConfig)
