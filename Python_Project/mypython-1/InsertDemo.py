#!/usr/bin/python3
# -*-coding:utf-8-*-

import pymysql

try:
    # 开启链接
    conn = pymysql.connect(host='localhost',user='root',passwd='123',
                           db='python',port=3306,charset='utf8')
    # 打开游标
    cur = conn.cursor()

    i = 0
    while i < 10000:
        sql = "insert into t1(name,age) VALUES ('%s','%d')" % ('tom' + str(i), i % 100)
        print(sql)
        cur.execute(sql)
        i += 1

    conn.commit()
    cur.close()
    conn.close()

except Exception:
    print("发生异常")

