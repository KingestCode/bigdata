#!/usr/bin/python3
# -*-coding:utf-8-*-


# =========================
# 输出相关
# =========================



# 输出整个字符串
print("hello, world")


print('\\\t\\')
print("\\\t\\")
print('''\\\t\\''')
print("""\\\t\\""")
print(r'\\\t\\')
print(R"\\\t\\")


# 输出多个字符串，结果会进行拼接
print("woai beijing tiananmen")
print("woai", "beijing", "tiananmen")


# 输出表达式的结果
print(300)
print(100 + 200)
result_num = 100 + 200
print(result_num)
# 打印输出：100 + 200 =  300
print("100 + 200 =", (100 + 200))


# 输出布尔值
print(True)
print(False)


# 输出多个变量，结果是这多个字符串变量按照空格拼接的结果
a = "我"
b = "爱"
c = "北京"
d = "天安门"
print(a, b, c, d)
print(a, b, c, d, end="\n")
print(a, b, c, d, end=",")
print(a, b, c, d, sep="")


# 带占位符的输出

aa = "huangbo"
bb = 44
print("His name is %s, his age is %d" % (aa, bb))



print("-----------------------------")

#

"""
如果是 %nd  那么补齐该数值变量为  n  位，  前几位空缺的都是使用 0 补齐
如果是 %ns  那么补齐该字符串变量为  n位，  前几位孔泉的都是使用 "" 补齐
"""
print('%3d-%03d' % (3, 1))
print('%.4f' % 3.1415926)
print('%02s-%3d' % ("1", "2"))
print('Hello, {0}, 股价上升了 {1:.2f}%'.format('小刘', 17.456))