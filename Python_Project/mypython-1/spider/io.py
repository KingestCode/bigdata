#!/usr/bin/python3
# -*-coding:utf-8-*-

# 方式1
# 注意 open 的参数
f = open("/tmp/hello.txt",mode="w",encoding="utf-8")
f.write("hello python")
f.close()


# 方式2
with open("/tmp/hello.txt",mode="w",encoding="utf-8") as f:
    f.write("hello pythonnnnnnn")


