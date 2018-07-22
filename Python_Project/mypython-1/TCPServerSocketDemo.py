# -*- encoding=utf-8 -*-
import socket

#创建socket对象SOCK_STREAM表示tcp协议。
ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

#绑定地址
ss.bind(("localhost",8888))

#启动监听
ss.listen(0)

print("启动监听了！！")

buffer=1024

while True :
    #接受连接
    (sock,addr) = ss.accept()
    print("有连接了 : " + str(addr))

    while True :
        data = sock.recv(buffer)
        #对bytes(字节数组),decode解码字符串.
        i = int.from_bytes(data,byteorder='little')
        print("recved : " + str(i)) ;
    print("收到数据了: %s" % (s))

