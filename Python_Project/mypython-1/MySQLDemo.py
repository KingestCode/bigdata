#!/usr/bin/python3
# -*-coding:utf-8-*-

import pymysql
print("hello world")


try:
    # 开启链接
    conn = pymysql.connect(host='localhost',user='root',passwd='123',
                           db='python',port=3306,charset='utf8')
    # 打开游标
    cur = conn.cursor()

    # 执行 sql
    cur.execute('select version()')

    version = cur.fetchone()

    print(version)
    cur.close()
    conn.close()

except Exception:
    print("发生异常")

