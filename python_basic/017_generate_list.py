#!/usr/bin/python3
# -*-coding:utf-8-*-



"""
列表生成式

https://www.liaoxuefeng.com/wiki/0014316089557264a6b348958f449949df42a6d3a2e542c000/001431779637539089fd627094a43a8a7c77e6102e3a811000
"""
# 列表生成式 & 列表生成器

>>> [x * x for x in range(1, 11)]
[1, 4, 9, 16, 25, 36, 49, 64, 81, 100]

# for循环后面还可以加上if判断，这样我们就可以筛选出仅偶数的平方：
>>> [x * x for x in range(1, 11) if x % 2 == 0]
[4, 16, 36, 64, 100]


# 还可以使用两层循环，可以生成全排列：
>>> [m + n for m in 'ABC' for n in 'XYZ']
['AX', 'AY', 'AZ', 'BX', 'BY', 'BZ', 'CX', 'CY', 'CZ']



g = (x for x in range(10,21))
print(g)

print(type(g))

print(next(g))


