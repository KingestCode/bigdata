#!/usr/bin/python3
# -*-coding:utf-8-*-
# author: https://blog.csdn.net/zhongqi2513

# ====================================================
# 内容描述： 使用 正则表达式 抓取名言网的名言
# ====================================================

from urllib.request import urlopen
# 如果需要解决异常， 则引入对应的异常类
from urllib.request import URLError
# 引入一个正则表达式模块，用来解决解析网页的问题
import re

url = "http://quotes.toscrape.com/"

try:
    response = urlopen(url)
except:
    print("请求URL出现问题")
else:
    html = response.read()
    html = html.decode("UTF-8")
    # print(html)

    # 抓取网页内容中的 名言标签， 利用正则的分组技术，只保留 html 内容
    quotes = re.findall('<span class="text" itemprop="text">(.*)</span>', html)
    # for x in quotes:
    #     print(x)

    # 抓取 作者信息
    author = re.findall('<small class="author" itemprop="author">(.*)</small>', html)
    # for y in author:
    #     print(y)

    # 抓取标签
    # 直接抓取meta信息抓取不到，放弃， 抓取其下的 a 标签中的内容
    # tags = re.findall('<meta class="keywords" itemprop="keywords" content="(.*)">', html)
    # 这种情况，换行符通过.*的方式是匹配不出来的。
    # tags = re.findall('<div class="tags">(.*)</div>', html)
    tags = re.findall('<div class="tags">(.*?)</div>', html, re.RegexFlag.DOTALL)
    # print(len(tags))
    true_tags = []
    for tag in tags:
        # print(tag)
        atag = re.findall('<a class="tag" href=".*">(.*)</a>', tag)
        # print(atag)
        true_tags.append(atag)

    last = []
    # 整合最终的结果
    for index in range(len(quotes)):
        q = quotes[index].strip("“”")
        a = author[index]
        t = ",".join(true_tags[index])
        print(q, a, t, sep="\t\t")

finally:
    print("请求完毕")

