#!/usr/bin/python3
# -*-coding:utf-8-*-




# 数据类型 --- List类型

# 列表操作相关

# Python 内置的一种数据类型是列表：list。 list 是一种有序的集合，可以随时添加和删除其中的元素。
# 创建一个列表，只要把逗号分隔的不同的数据项使用方括号括起来即可,且列表的数据项不需要具有相同的类型



## 定义 list
l1 = list((1,2,3,4,5))
l2 = list(["huangbo",'xuzheng',1234, 55.55])
l3 = ["huangbo",'xuzheng',1234, 55.55]


## 访问list
print(l3[0])
print(type(l3[0]))
print(l3[1])
print(type(l3[1]))
print(l3[2])
print(type(l3[2]))
print(l3[3])
print(type(l3[3]))


## list切片
print(l3[1:4])
print(l3[2:3])


## 更新list
print(l3)
l3[1] = "wangbaoqiang"
print(l3)
l3.append("xuzheng")
print(l3)


## 删除list
print(l3)
del l3[2]
print(l3)


## list运算符
print(len([1,2,3,4,5,6,7,8,9]))
l4 = [1,2,3] + [4,5,6]
print(l4)
print(l4 * 4)
print(4 in l4)
print(7 in l4)


## list遍历
for x in l4:
    print(x, end=" ")

print()



## list的其他函数
l6 = [3,4,5,6,7,6,6]
# len(list)	列表元素个数
print(len(l6))
# max(list)	返回列表元素最大值
print(max(l6))
# min(list)	返回列表元素最小值
print(min(l6))
# list(seq)	将元组转换为列表
print(list(range(1,10)))
# list.count(obj)	统计某个元素在列表中出现的次数
print(l6.count(6))
print(l6.count(1))
# list.index(obj)	从列表中找出某个值第一个匹配项的索引位置
print(l6.index(4))
# list.append(obj)	在列表末尾添加新的对象
l6.append(44)
print(l6)
# list.insert(index, obj)	将对象插入列表
l6.insert(1,"huangbo")
print(l6)
# list.pop(obj=list[-1])	移除列表中的一个元素（默认最后一个元素），并且返回该元素的值
print(l6.pop())
print(l6)
print(l6.pop(1))
print(l6)
# list.reverse()	反向列表中元素
l6.reverse()
print(l6)
# list.sort([func])	对原列表进行排序
l6.sort()
print(l6)
l6.sort(reverse=True)
print(l6)
