#!/usr/bin/python3
# -*-coding:utf-8-*-




# 变量定义相关

# 定义变量，不用声明类型， python是一种动态语言。具有类型推断功能， 能自动识别出aa变量是字符串类型
aa = "huangbo"
aa = 11


# 自动识别 bb变量 为  数值类型
bb = 11


# python的优良特性，可以在一行声明多个变量
cc, dd = "xuzheng", 22
print(cc, dd)


# 实现cc和dd变量的交换
cc, dd = dd, cc
print(cc, dd)


# 连环声明
ee = ff = gg = hh = 12


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