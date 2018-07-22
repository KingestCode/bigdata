#!/usr/bin/python3
# -*-coding:utf-8-*-

import pymysql

try:
    # 开启链接
    conn = pymysql.connect(host='localhost',user='root',passwd='123',
                           db='python',port=3306,charset='utf8')
    # 打开游标
    cur = conn.cursor()

    sql = "select id, name,age from t1 where name like 'tom8%'"

    # 执行 sql
    cur.execute(sql)

    # 取结果
    all = cur.fetchall()

    for rec in all:
        print(rec)
        # print(str(rec[0]))

    conn.commit()
    cur.close()
    conn.close()

except Exception:
    print("发生异常")

