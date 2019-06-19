#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/6/13 8:47
# @Author  : Fcvane
# @Param   : 
# @File    : XmlAnalysis.py

import PasswdUtil
import os, sys
import LogUtil
import VariableUtil

try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree

'''
解析数据库参数集目录为：../conf/dbParams
格式为：
<?xml version='1.0' encoding='utf-8'?>
<configuration>
    <db type="oracle">
        <host>10.45.15.20</host>
        <port>1521</port>
        <sid>orcl</sid>
        <serverName></serverName>
        <userName>scott</userName>
        <passWord>xoiMDvbRNj8hS8f7QkVdwR5VTVnNHiLBXKEPE/pd2DyYxwW0gVL3VYiw6Mr6hv9+hF5TqD5/WGc4wuolwHup9Q==</passWord>
        <enable>Ture</enable>
    </db>
</configuration>
'''

name = os.path.basename(__file__)


def dbCFGInfo(program):
    taskName = VariableUtil.DB_PATH + os.sep + 'db_{program}.xml'.format(program=program)
    LogUtil.log(name, 'Start to analysis {taskName}'.format(taskName=taskName), 'info')
    result = {}
    try:
        tree = etree.parse(taskName)
        # 获得子元素
        elemlist = tree.findall('db')
        # 遍历task所有子元素
        for elem in elemlist:
            array = {}
            for child in elem.getchildren():
                # print (child.tag, ":", child.attrib, ":", child.text)
                array[child.tag] = child.text
            result[elem.attrib['type']] = array
            # print (array,"-----",result)
        LogUtil.log(name, 'File {taskName} analysis success "'.format(taskName=taskName), 'info')
    except Exception as e:  # 捕获除与程序退出sys.exit()相关之外的所有异常
        LogUtil.log(name, 'File {taskName} analysis failure '.format(taskName=taskName), 'error')
        sys.exit()
    # print(result)
    return result


if __name__ == '__main__':
    dbConfig = dbCFGInfo('commondb')
    print(dbConfig)
