#!/usr/bin/python3
# -*-coding:utf-8-*-

# ====================================================
# 内容描述： 必应翻译 应用
# ====================================================

from urllib.request import urlopen
from urllib.parse import urlencode
from bs4 import BeautifulSoup

# 必应词典的请求链接样式：

'''
https://cn.bing.com/dict/search?q=desk
'''

def fanyi(key):

    param = {"q" : key}
    newkey = urlencode(param)
    url = "https://cn.bing.com/dict/search?" + newkey

    response = urlopen(url)
    bs = BeautifulSoup(response, "html.parser")

    # html = response.read().decode("UTF-8")
    # print(html)

    lis = bs.select("div.qdef > ul > li")

    li_content = []
    for li in lis:
        li_content.append(li.text)
        print(li.text)
        # print(li)
    # print(li_content)


while True:
    # key = "desk"
    # url = "https://cn.bing.com/dict/search?q=desk"
    key = input("please input you key : ")
    if key == "bye":
        print("感谢使用SS牌翻译软件")
        break
    fanyi(key)