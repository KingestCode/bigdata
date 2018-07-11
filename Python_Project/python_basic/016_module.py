#!/usr/bin/python3
# -*-coding:utf-8-*-


# 模块相关
'''

在python中一个文件可以被看成一个独立模块，而包对应着文件夹，
模块把python代码分成一些有组织的代码段，通过导入的方式实现代码重用。


在 python 用 import 或者 from...import... 来导入相应的模块。
将整个模块(somemodule)导入，格式为： import somemodule
从某个模块中导入某个函数,格式为： from somemodule import somefunction
从某个模块中导入多个函数,格式为： from somemodule import firstfunc, secondfunc, thirdfunc
将某个模块中的全部函数导入，格式为： from somemodule import *

起别名:
import somemodule as sm

from somemodule inport somefunction as sf

'''

# 正确的写法
import os
import sys
# 不推荐的写法
import sys,os


# 正确的写法
from subprocess import Popen, PIPE
from urllib.request import *


# 正确的写法
from urllib.request import urlopen


# 给引入的模块或者函数取别名
# import numpy as np
from urllib.request import urlopen as uo
uo("http://www.baidu.com")



## 引用其他的模块
# 导入: import 文件名
# 或者 from 文件名 import 函数名
# 使用: 文件名.函数名(..)



"""
常用内置模块

https://www.liaoxuefeng.com/wiki/0014316089557264a6b348958f449949df42a6d3a2e542c000/0014319347182373b696e637cc04430b8ee2d548ca1b36d000
"""




"""
安装第三方模块

https://www.liaoxuefeng.com/wiki/0014316089557264a6b348958f449949df42a6d3a2e542c000/001432002680493d1babda364904ca0a6e28374498d59a7000
"""





import os
[print(d) for d in os.listdir('c:/')]



for x in "huangbo":
    print(x)



ddd = {'x': 'A', 'y': 'B', 'z': 'C' }
keys = ddd.keys()
for xx in  keys:
    print(xx, ddd.get(xx))