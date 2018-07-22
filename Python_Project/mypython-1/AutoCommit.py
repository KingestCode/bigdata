#!/usr/bin/python3
# -*-coding:utf-8-*-

import pymysql

try:
    # 开启连接
    conn = pymysql.connect(host='localhost',user='root',passwd='123',db='python',port=3306,charset='utf8')

    # 关闭自动提交
    conn.autocommit(False)

    #开启事务
    conn.begin()
    # 打开游标
    cur = conn.cursor()

    # 删除
    sql = "delete from t1 WHERE id > 20000"
    # 改
    sql = "update t1 set age = age -1 where age >=50 "

    # 聚合
    sql = "select count(*) from t1 where age < 20"

    # 执行 sql
    cur.execute(sql)

    # 有结果的, 执行完后需要fetch结果
    res = cur.fetchone()
    print(res[0])

    # 提交连接
    conn.commit()
    # 关闭游标
    cur.close()

except Exception:
    print("发生异常")
    conn.rollback()

finally:
    conn.close()

