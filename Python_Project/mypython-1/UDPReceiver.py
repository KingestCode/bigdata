# -*- encoding=utf-8 -*-
import socket

# 创建socket对象SOCK_DGRAM表示udp协议。
recver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
recver.bind(("localhost", 8888))
print("启动了udp接受者")
while True:
    (data, addr) = recver.recvfrom(1024)
    msg = bytes.decode(data);

    print("接收了 from " + str(addr[0]) + ":" + str(addr[1]) + " : " + msg)

