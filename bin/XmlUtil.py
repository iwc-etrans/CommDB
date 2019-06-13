#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/6/13 8:47
# @Author  : Fcvane
# @Param   : 
# @File    : XmlAnalysis.py

import sys
import os
import datetime
import log
import VariableUtil
try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree

'''
解析数据库参数集目录为：../conf/dbParams
格式为：
<?xml version='1.0' encoding='utf-8'?>
<conf>
  <auth db="ORA_SCOTT_LOCALHOST">
    <jdbcurl>jdbc:oracle:thin:@10.45.15.205:1521/orcl</jdbcurl>
    <username>scott</username>
    <password>xoiMDvbRNj8hS8f7QkVdwR5VTVnNHiLBXKEPE/pd2DyYxwW0gVL3VYiw6Mr6hv9+hF5TqD5/WGc4wuolwHup9Q==</password>
  </auth>
</conf>
'''
def dbCFGInfo(program):
    CONF_PATH = VariableUtil.CONF_PATH
    taskName = CONF_PATH + os.sep + 'db_{program}.xml'.format(program = program)
    logname="XmlAnalysis_LogDetail"
    log(logname,'parse %s 目录下的db_%s.xml文件.' % (CONF_PATH, program),"info")
    result = {}
    try:
        log(logname, '%s parse start ...' % taskName, "info")
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
    except Exception as e:  # 捕获除与程序退出sys.exit()相关之外的所有异常
        log(logname, 'parse test.xml fail !!.', "error")
        #print('parse test.xml fail !!.')
        sys.exit()
    # print(result)
    return result

if __name__ == '__main__':
    # programConfig = parseCFGInfo('fircus_dkh',  'job_config.xml')
    # print (programConfig)
    dbConfig = dbCFGInfo('fircus_dkh')
    print(dbConfig)
