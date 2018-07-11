#!/usr/bin/python3
# -*-coding:utf-8-*-



# 数据类型 --- Number类型

"""

Python3 支持 int整型、float浮点型、bool布尔型、complex复数 四种数值类型。

在Python 3里，只有一种整数类型 int，表示为长整型，没有 python2 中的 Long。
像大多数语言一样，数值类型的赋值和计算都是很直观的。
内置的 type() 函数可以用来查询变量所指的对象类型。

"""


# 查看某个变量是什么类型  type() 函数
a, b, c, d = 20, 5.5, True, 4+3j
print(type(a), type(b), type(c), type(d))



# 比对某个变量是否是对应的参数类型   isinstance() 函数
aa = 111
print(isinstance(aa, int))



print("-----------------------------------")

# isinstance和type的区别 :
#       type()不会认为子类是一种父类类型,
#       isinstance()会认为子类是一种父类类型
class A:
    pass

class B(A):
    pass

print(isinstance(A(), A))      # returns True
print(type(A()) == A)          # returns True
print(isinstance(B(), A))      # returns True
print(type(B()) == A)          # returns False


#  ord函数返回一个字符串的ASCII编码值
print(ord("A"))
print(ord("a"))
print(ord("1"))

#  chr函数返回一个数值的ASCII字符
print(chr(65))
print(chr(97))
print(chr(49))





"""
int(x [,base ])	将x转换为一个整数
float(x )	将x转换到一个浮点数
complex(real [,imag ])	创建一个复数
str(x )	将对象 x 转换为字符串
repr(x )	将对象 x 转换为表达式字符串
eval(str )	用来计算在字符串中的有效 Python 表达式,并返回一个对象
tuple(s )	将序列 s 转换为一个元组
list(s )	将序列 s 转换为一个列表
chr(x )	将一个整数转换为一个字符
unichr(x )	将一个整数转换为 Unicode 字符
ord(x )	将一个字符转换为它的整数值
hex(x )	将一个整数转换为一个十六进制字符串
oct(x )	将一个整数转换为一个八进制字符串
"""
print(int("17"));
print(int("17",10));
print(int("17",16));
print(str(23))
# .....
