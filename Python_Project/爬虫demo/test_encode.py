#!/usr/bin/python3
# -*-coding:utf-8-*-




from urllib.parse import urlencode

param = {"birthday":"生日快乐", "mingxing":"huangbo"}
result = urlencode(param)
print(result)