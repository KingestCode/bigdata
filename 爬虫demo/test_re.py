#!/usr/bin/python3
# -*-coding:utf-8-*-


import re

str = "aaaaaa"
result = re.findall('aa+?', str)
print(result)


result1 = re.findall('aa+', str)
print(result1)
