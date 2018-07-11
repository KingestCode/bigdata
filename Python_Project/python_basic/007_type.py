#!/usr/bin/python3
# -*-coding:utf-8-*-



# 数据类型

"""

Python3 中有六个标准的数据类型：

    Number（数字）
    String（字符串）
    List（列表）
    Tuple（元组）
    Sets（集合）
    Dictionary（字典）

"""

# 声明数值类型
num_1 = 1
num_2 = 3.1415926
num_3 = True
num_4 = 3 + 4j
print(num_1, num_2, num_3, num_4)


# 声明字符串类型
str_1 = "huangbo"
str_2 = 'a'
print(str_1, str_2)
print(type(str_1), type(str_2))


# 声明list
list = [1,2,3,4,"huangbo",True]
print(list)
print(type(list))


# 声明元组
tuple=(1,2,3,4,"huangbo")
print(tuple)
print(type(tuple))


# 声明集合
set = {"a",1,"b",2,"c",3}
print(set)
print(type(set))


# 声明字典
dict = {"a":1,"b":2,"c":3}
print(dict)
print(type(dict))