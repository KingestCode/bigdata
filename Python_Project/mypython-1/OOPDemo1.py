#!/usr/bin/python3
# -*-coding:utf-8-*-

# 定义类
class Dog:
    #类似于 java 中的静态成员,直接通过类访问
    name = "dahuang"
    __color = "yello"


    #定义构造函数
    def __init__(self,age):
        self.__color = "blue"
        print("new Dog()....")

        #相当于添加实例变量
        self.age = age

        # py中可以通过 del 删除变量
        # del self.age

    def __del__(self):
        print("销毁对象了")

    def watch(self):
        print("watch house!")


    def add(self,a,b):
        return a+b


#创建对象, 相当于 Java 中的 new Dog()
# 构造函数带参的话, 要传参
d1 = Dog(13)

# 访问静态成员
print(Dog.name)

# 调用实例方法
d1.watch()

# 访问实例对象的方法
print(d1.add(1,2))

# 访问实例变量
print(d1.age)
print(d1.name)

# 判断对象是否有指定的属性
print(hasattr(d1,"age"))

# py 内置函数,可以删除属性
# 删除属性
delattr(d1,"age")


dict1 = Dog.__dict__
for k,v in dict1.items():
    print(k,v)

print("++++++++")

for k in dict1.keys():
    print(k)

#删除对象, 调用析构函数, 类似于 java 中的 finalize()
# del d1
# d1 = None

print(" 继承 Kk")

class Cat:
    def catchMouse(self):
        print("抓了个老鼠!!")
        self.__eatFish()

    # 定义私有方法, 前面加上__, 只能在当前对象中访问
    def __eatFish(self):
         print("好吃")

# c = Cat()
# c.catchMouse()

# 此方法可以被其它包导入
def haha():
    print("ahah")


class DogCat(Dog, Cat):

    @staticmethod
    def HH(s):
        print("static....")

    def __init__(self):
        # 如果多继承的父类带参, 这里要在 init 方法中显示调用
        Dog.__init__(self,12)

    def run(self):
        print(".......")

    def catchMouse(self):
        print("抓了个青蛙")

dc = DogCat()
dc.watch()
dc.catchMouse()




