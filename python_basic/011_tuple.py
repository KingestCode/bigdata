#!/usr/bin/python3
# -*-coding:utf-8-*-





# 数据类型 --- Tuple类型
# 另一种有序列表叫元组：tuple 。tuple 和 list 非常类似，但是 tuple 一旦初始化就不能修改。
# tuple 不可变是指当你创建了 tuple 时候，它就不能改变了，也就是说它也没有 append()，insert() 这样的方法，但它也有获取某个索引值的方法，但是不能赋值。
# 那么为什么要有 tuple 呢？那是因为 tuple 是不可变的，所以代码更安全。所以建议能用 tuple 代替 list 就尽量用 tuple 。

# 元组操作相关


t1 = 1, 2, 3, 4
print(t1)
print(type(t1))
print(t1[1])
print("----------------------------------")


t2 = ("1", 2, "abc")
print(t2)
print(type(t2))
print(t2[1])
print("----------------------------------")



t3 = 1
print(t3)
print(type(t3))
print("----------------------------------")


t4 = 2,
print(t4)
print(type(t4))
print("----------------------------------")


# 注意：元组是不可更改的, 如果每一个item是简单类型的话，那么值是不可以更改的，
# 但是如果是复杂类型，比如是list列表，那么列表不可以更改，但是列表中的元素可以更改
# t2[2] = 3
# print(t2[2])

t5 = (1, 2, [3, 4])
print(t5)
print(type(t5))
t5[2][0] = "X"
t5[2][1] = "Y"
print(t5)
print(type(t5))
print("----------------------------------")



## 元组其他操作
list1 = [1,2,3]
t6 = (1, 2.0, "huangbo", list1)
t7 = (1, 2.0, "huangbo", list1)
print(len(t6))
print((1, 2, 3) + (4, 5, 6))
print(("huangbo",) * 4)
print("huangbo" in  t6)



## 元组遍历
for x in t6:
    print(x)