#!/usr/bin/python3
# -*-coding:utf-8-*-



# 生成器相关

"""
https://www.liaoxuefeng.com/wiki/0014316089557264a6b348958f449949df42a6d3a2e542c000/0014317799226173f45ce40636141b6abc8424e12b5fb27000
"""

# iterable 对象, 可通过 iter() 转为 iterator

print(isinstance(iter([]) ,Iterator))
print(isinstance(iter("abc") ,Iterator))
