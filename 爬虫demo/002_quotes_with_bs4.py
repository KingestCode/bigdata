#!/usr/bin/python3
# -*-coding:utf-8-*-
# author: https://blog.csdn.net/zhongqi2513

# ====================================================
# 内容描述： 使用 BeautifulSoup4 抓取名言网的名言
# ====================================================

from urllib.request import urlopen
from bs4 import BeautifulSoup

url = "http://quotes.toscrape.com/"
response = urlopen(url)
# html = response.read().decode("UTF-8")
# print(html)

# 在构造 BeautifulSoup 对象的时候，需要传入两个参数，
# 第一个： 浏览器返回的response对象
# 第二个： 需要一个解析器， 解析器有很多种，但是通常使用默认的 html.parser 即可
# BeautifulSoup的解析器还有lxml和html5lib等
bs = BeautifulSoup(response, "html.parser")

# 抓取 名言
spans = bs.select("span.text")
true_quotes = []
for one in spans:
    # print(one.get("text", "aaa"))
    quote = one.text
    # print("abcdeeee e".strip("abe"))  去除两边的指定字符
    quote = quote.strip("“”")
    true_quotes.append(quote)
    # print(one.text)


# 抓取作者
true_authors = []
authors = bs.select("small.author")
for author in authors:
    true_authors.append(author.text)
    # print(author.text)


# 抓取标签
tags = bs.select("div.tags")
true_tags = []
for tag in tags:
    a = tag.select("a.tag")

    # 第一，直接打印，发现是 一个数组
    # print(author)

    # 第二，把所有的标签组合到一个list中
    # tagss = []
    # for tag in a:
    #     tagss.append(tag.text)
    # print(tagss)

    # 第三： 使用生成器的语法规则，生成列表
    mytagss = [atag.text for atag in a]
    result = ",".join(mytagss)
    true_tags.append(result)
    # print(result)

# print(true_authors)
# print(true_tags)
# print(true_quotes)

for index in range(len(true_quotes)):
    print("\t".join([true_authors[index], true_tags[index], true_quotes[index]]))


