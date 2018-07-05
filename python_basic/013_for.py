#!/usr/bin/python3
# -*-coding:utf-8-*-



# for 迭代相关
# for循环可以遍历任何序列的项目，如一个列表或者一个字符串


# range函数

print(range(10))
print(range(3, 10))
print(range(3, 10, 2))


for a in range(10):
    print(a)
print("------------------------------")

for a in range(3, 10):
    print(a)
print("------------------------------")

for a in range(3, 10, 2):
    print(a)

print("------------------------------")
# 拓展：
for a in range(10, 0, -2):
    print(a)




aa = ["huangbo", "xuzheng", "wangbaoqiang", 44]
print("------------------------------")
# for 遍历列表：第一种，使用in进行遍历
for a in aa:
    print(a)
print("------------------------------")
# for 遍历列表：第二种，使用下标进行遍历
for i in range(len(aa)):
    print(aa[i])



# 遍历字符串
for letter in "huangbo":
    print(letter)



"""
有 while … else 语句，当然也有 for … else 语句啦，for 中的语句和普通的没有区别，
else 中的语句会在循环正常执行完（即 for 不是通过 break 跳出而中断的）的情况下执行，while … else 也是一样。
"""
for num in range(10, 20):           # 迭代 10 到 20 之间的数字
   for i in range(2, num):          # 根据因子迭代
      if num % i == 0:              # 确定是否能除尽，能除尽，则表示是合数
         print('%d 是一个合数' %num)
         break                      # 跳出当前循环
   else:                            # 循环的 else 部分
      print('%d 是一个质数' %num)






##  条件语句和循环语句综合实例

# 打印99乘法表
for i in range(1, 10):
    for j in range(1, i+1):
        # 打印语句中，大括号及其里面的字符 (称作格式化字段) 将会被 .format() 中的参数替换,注意有个点的
        print("{0}*{1}={2}\t".format(i, j, i*j), end="")
    print()


## 一句话打印99乘法表
print ('\n'.join([' '.join(['%s*%s=%-3s' % (y, x, x*y) for y in range(1, x+1)]) for x in range(1, 10)]))




## 判断是否是 闰年
year = int(input("请输入一个年份: "))
if (year % 4) == 0 and (year % 100) != 0 or (year % 400) == 0:
    print('{0} 是闰年' .format(year))
else:
    print('{0} 不是闰年' .format(year))