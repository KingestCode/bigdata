#!/usr/bin/python3
# -*-coding:utf-8-*-
# author: https://blog.csdn.net/zhongqi2513

# ====================================================
# 内容描述： 中国天气网 北京天气 抓取 打印输出
# ====================================================

from urllib.request import urlopen
from bs4 import BeautifulSoup


url = "http://www.weather.com.cn/weather/101010100.shtml"

response = urlopen(url)

bs = BeautifulSoup(response, "html.parser")

# 按照顺序依次找出五列数据：   日期date， 描述 desc,  温度temp    风向direction  level 风力
date_list = bs.select("li > h1")
desc_list = bs.select("li > p.wea")
temp_list = bs.select("li > p.tem")
direction_list = bs.select("li > p.win > em")
level_list = bs.select("li > p.win > i")

result_list = []
for i in range(len(date_list)):
    # 日期
    date = date_list[i].text

    # 描述
    desc = desc_list[i].text

    # 温度
    # temp = temp_list[i].text.strip().replace("\n", "")            # 这一句代码等同于下面两句代码
    temp = temp_list[i].stripped_strings
    temp = "".join(temp)

    # 风向
    direction_temp = direction_list[i]
    two_span_list = direction_temp.select("span")
    if len(two_span_list) == 1:
        direction = two_span_list[0].get("title")
    else:
        direction = two_span_list[0].get("title") + "-" + two_span_list[1].get("title")

    # 风力
    level = level_list[i].text

    result_list.append([date, desc, temp,direction,level])
    # print(date, desc, temp,direction,level, sep="\t")


for result in result_list:
    print("\t".join(result))