# -*- encoding=utf-8 -*-

import socket ;

#创建socket对象
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#连接到server端
sock.connect(("localhost",8888))

# 构造字节数组数据b"hello world"
# sock.send(b"hello world")
index = 0 ;
while True :
    b = index.to_bytes(4,byteorder='little')
    #发送数据
    sock.send(b)
    print("sended : " + str(index))
    index += 1 ;

    import time ;
    time.sleep(1)

sock.close();

