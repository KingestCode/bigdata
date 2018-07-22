#!/usr/bin/python3
# -*-coding:utf-8-*-

# 多线程卖票

import threading

tickets = 1000

loc = threading.Lock()


def getTicket():
    global loc

    loc.acquire()

    global tickets
    tmp = 0;
    if tickets > 0:
        tmp = tickets
        tickets -= 1
    else:
        tmp = 0
    loc.release()
    return tmp


class Saler(threading.Thread):
    def run(self):
        global tickets
        while True:
            tick = getTicket()
            if tick != 0:
                print(self.getName() + ":" + str(tick))
            else:
                return


s1 = Saler()
s1.setName("s1")

s2 = Saler()
s2.setName("s2")

s1.start()
s2.start()
