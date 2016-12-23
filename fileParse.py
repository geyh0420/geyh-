#!/usr/bin/env python
#  -*- coding: UTF-8 -*-

import glob
import re
import mysql.connector
import datetime
import time

def conn_apmsql():

    conn = None
    try:
        # 链接数据库
        conn = mysql.connector.connect(
                host='localhost',
                port = 3306,
                user='root',
                passwd='123456',
                db='apm',
                charset="utf8",
                )
    except:
        return None

    return conn

def lineParse(line, cursor):

    i_len = len(line)

    if(i_len > 2048):
        return
    #获取协议名
    index = line.find("\t", 0, i_len)
    if(line.find("http", 0, index)!= -1):
        s_proto = "http"
    elif(line.find("mysql", 0, index)!= -1):
        s_proto = "mysql"
    else:
        return

    #获取开始时间
    index = index + 1
    index_tmp = line.find("\t", index, i_len)
    startTime = line[index:index_tmp]
    try:
        mstime = long(startTime) % 1000
    except:
        return

    ltime=time.localtime(float(startTime)/1000)
    timeStr=time.strftime("%Y-%m-%d %H:%M:%S", ltime)
    strms = str(mstime)
    mstime = strms.zfill(3)
    startTime = timeStr + '.' + mstime

    index = index_tmp + 1
    index_tmp = line.find("\t", index, i_len)
    endTime = line[index:index_tmp]
    try:
        mstime = long(endTime) % 1000
    except:
        return

    ltime=time.localtime(float(endTime)/1000)
    timeStr=time.strftime("%Y-%m-%d %H:%M:%S", ltime)
    strms = str(mstime)
    mstime = strms.zfill(3)
    endTime = timeStr + '.' + mstime

    index = index_tmp + 1
    index_tmp = line.find("\t", index, i_len)
    src = line[index:index_tmp]

    index = src.find(":", 0, len(src))

    srcIp = src[0:index]
    srcPort = src[index + 1:len(src)]

    index = index_tmp + 1
    index_tmp = line.find("\t", index, i_len)
    dst = line[index:index_tmp]

    index = dst.find(":", 0, len(src))

    dstIp = dst[0:index]
    dstPort = dst[index + 1:len(src)]

    if(s_proto == "mysql"):
        index = index_tmp + 1
        index_tmp = line.find("\t", index, i_len)
        title = line[index:index_tmp]

        index = index_tmp + 1
        sqlText = line[index:i_len]

    else:
        index = index_tmp + 1
        title = line[index:i_len]
        sqlText = None

    if s_proto == "mysql":
        insert_sql_mysql = '''INSERT IGNORE INTO mysql(proto ,startTime ,endTime ,srcIp ,srcPort,
                                                       dstIp , dstPort ,title ,sqlText)  values
                                                       (%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s)'''
        cursor.execute(insert_sql_mysql, [s_proto
                                        , startTime
                                        , endTime
                                        , srcIp
                                        , srcPort
                                        , dstIp
                                        , dstPort
                                        , title
                                        , sqlText])

    else:
        insert_sql_http = '''INSERT IGNORE INTO http(proto ,startTime ,endTime ,srcIp ,srcPort,
                                               dstIp , dstPort ,title)  values
                                               (%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s)'''

        cursor.execute(insert_sql_http, [s_proto
                                        , startTime
                                        , endTime
                                        , srcIp
                                        , srcPort
                                        , dstIp
                                        , dstPort
                                        , title])
    return

#获取文件夹
fileNameList = []
for filename in glob.glob(r"./lua.log.*"):
    fileNameList.append(filename)

print fileNameList

#循环解码文件
for filename in fileNameList:
    mysqlConn = conn_apmsql()
    if mysqlConn == None:
        print "connect mysql failed\n"
    else:
        print "connect mysql succ\n"

    cursor = mysqlConn.cursor()
    file = open(filename, "r")
    line = file.readline(10240)
    while line:
        #循环解析
        lineParse(line, cursor)
        line = file.readline()
        mysqlConn.commit()
    file.close()
    cursor.close()
    mysqlConn.close()
