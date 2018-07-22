# -*- encoding=utf-8 -*-
import socket

#创建socket对象SOCK_DGRAM表示udp协议。
sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sender.bind(("localhost",9999))
print("启动了udp发送者")
index = 0 ;
while True:
    msg = "hello" + str(index) ;
    msgBytes = msg.encode("utf-8");
    sender.sendto(msgBytes,("localhost",8888))
    print("发送了 : " + msg)
    index += 1 ;
    import time
    time.sleep(1)

