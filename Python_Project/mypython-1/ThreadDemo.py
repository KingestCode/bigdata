#!/usr/bin/python3
# -*-coding:utf-8-*-

# 导入 time 模块
import time

# 高级线程接口
import threading

# 低级接口
import _thread

def hello():
    # 获取当前线程名字
    tname = threading.current_thread().getName()
    for i in [1,2,3,4,5,6,7]:
        print(tname + ":" + str(i))

        import sys
        import os
        # 提取秒, 精确到微秒
        cc = int(time.time() * 1000)        # 转化成毫秒
        print(cc)


_thread.start_new_thread(hello,())
_thread.start_new_thread(hello,())

# 休眠2秒
time.sleep(2)