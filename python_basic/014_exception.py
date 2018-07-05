#!/usr/bin/python3
# -*-coding:utf-8-*-


"""
测试python中的异常语法结构
"""
from urllib.request import urlopen
from urllib.request import URLError as ue

try:
    print(2 + 2)
    2 / 0
    raise ue("XXX")
except ue:
	print("出现 ue 异常", ue)
except Exception as ex:
    print("出现异常", ex)
else:
    print("没有出现异常")
finally:
    print("程序执行完毕")