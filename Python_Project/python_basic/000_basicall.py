# -*- encoding=utf-8-*-

print("hello world")
print("你好,中国!")

if True:
    print("true");
else:
    print("false")      # 打印输出

"""
    xxxxx
    xxx
    sldjfasdfa
    sdf
    sadfsafd

    heello
"""
print("1"); print("2");
# print(str)

a =100 ;
print(a)

a = "hello"
print(a)

b  = c  = 1

print(b)
print(c)

a = 10

str0 = "hello"

print(str0[2:4])
print(str0[2:])
print(str0[:4])
print(str0[2:-1])

# 字符串重复操作,等价字符数组操作
print(str0 * 2)

#类似于数组
lst = [1,2,3,"hello"] ;
lst[3] = 100
print(lst.__len__())
print(lst * 5)

#tuple元组

t = (1,2,3,4)
#t[1] = 100 ;
print(t[0:4])

#字典,等价于java的map
d = {100:"tom",200:"tomas",300:"tomasLee"};
d[100] = "jerry"
print(d[100])

#类型转换
s = str(d)
print(s)

#
s = "300"

print(int(s) + 200)

#eval 求值
s = "(100 + 200) * 3 / (2 + 5)" ;
print(eval(s))


#定义序列
seq = 1,2,3,4 ;

#将序列变换成list
lst = list(seq)
print(lst)

#将seq变换成tuple
t = tuple(seq);
print(t)

print("===============")

seq2 = (1,2),(2,3),(3,4)
dd = dict(seq2);
print(dd)

#** 幂运算
print(0 ** 0)

# / 浮点除法
print(1 / 2)
print(1 // 2)

a = 3 ;
a **= 2     # 相当于 a 的
print(a)

print(0 ^ -1)

print(2 << 3)
print(-1 >> 4)


print(0 and 1)

print(1 not in t)

#is 是身份运算，等价于java的equals(内容)
a = 100 ;
b = 100 ;

print(a == b)

t1 = (1,2,3,4)
t2 = (1,2,3,4)
print(t1 is t2)

age = 2 ;

if age < 20:
    print("小孩子")
elif age > 70:
    print("老人")
else:
    print("中间人")

rows = [1,2,3,4,5,6,7,8,9]
cols = [1,2,3,4,5,6,7,8,9]
for r in rows :
    for c in cols:
        pass
        if c <= r :
            print("%d x %d = %d" % (c , r , (c * r)) ,end='\t')
    print()

print('===================')

r = 1 ;
while r <= 9 :
    c = 1 ;
    while c <= r :
        print("%d x %d = %d" % (c, r, (c * r)), end='\t')
        if c == 5:
            break ;
        c += 1
    print()
    r += 1

t1 = (1,2,3)
t2 = (2,3,"hello")
#del t1

print(t1 * 3)
print(t1[-0])

print(max(t1))
s = "hello world"
s = s[0:3] + "ttt"
print(s)

#r"" 表示的原样输出，不需要转义
s = r"hello\r\nworld"
print(s)

print("15 is 0x%x" % (15))
print(u"\u89f7\u0020world")

del lst[2]
lst[:-2]
#1,2,4
print(len(lst[:-2]))
lst = [1,2,3,4,5]
lst.append(8)
lst.insert(0,9)
lst.clear()
lst = [1,2,3,4,5]

#弹出索引位置的元素
lst.pop()

#删除的指定元素，不是索引
#lst.remove(8)
print(lst)

arr = [[1,2,3],[4,5,6],[7,8,9]]
print(arr)

list_2d = [[col for col in cols] for row in rows]
print(list_2d)
print(d)

for xx in d.values():
    print(xx)

#IO操作
f = open("d:/scala/test.txt")
lines = f.readlines()
for l in lines :
    print(l,end='')

f = open("d:/scala/test.txt",'a')
# while True :
#     l2 = f.readline();
#     if l2 == '':
#         break ;
#     else:
#         print(l2 , end = '')

f.write("how are you!")
f.closed

# f1 = open("d:/Koala.jpg",'rb')
# f2 = open("d:/KKK.jpg", 'wb');
#
# #读取所有字节
# while True:
#     data = f1.read(1024 * 100)
#     if data != b'':
#         f2.write(data);
#     else :
#         break ;
# f1.closed
# f2.closed


import os
os.renames("d:/KKK.jpg","d:/YYY.jpg")
