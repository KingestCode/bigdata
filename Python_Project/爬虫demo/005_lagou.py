#!/usr/bin/python3
# -*- coding:utf-8 -*-
# author: https://blog.csdn.net/zhongqi2513

# ============================================================
# 内容描述： 拉勾应用
# ============================================================

from urllib.request import urlopen
from urllib.request import Request
from bs4 import BeautifulSoup

url = "https://www.lagou.com/zhaopin/Python/?labelWords=label"
head = {"User-Agent":'Mozilla/5.0',
       "Cookie":"user_trace_token=20180103182645-99459851-f070-11e7-9fc4-5254005c3644; LGUID=20180103182645-9945a1b3-f070-11e7-9fc4-5254005c3644; WEBTJ-ID=20180327134408-16265fc301594-0c66caddeff5b9-6e11107f-3686400-16265fc30166a7; JSESSIONID=ABAAABAAAGFABEF733D37AB2482DBC67214869650457103; index_location_city=%E5%8C%97%E4%BA%AC; TG-TRACK-CODE=index_navigation; _gat=1; PRE_UTM=; PRE_HOST=; PRE_SITE=; PRE_LAND=https%3A%2F%2Fwww.lagou.com%2Fzhaopin%2FPython%2F%3FlabelWords%3Dlabel; SEARCH_ID=fe9635d3a0e845c7ac4b332b0f75efe1; _gid=GA1.2.1532909077.1522129430; Hm_lvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1522129465,1522153423,1522153427,1522153940; Hm_lpvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1522156310; _ga=GA1.2.1903431618.1514975204; LGSID=20180327211140-6334ffee-31c0-11e8-b64e-5254005c3644; LGRID=20180327211149-686ce6df-31c0-11e8-a168-525400f775ce"}
request = Request(url, headers=head)
# request = Request(url)

# response = urlopen("https://www.lagou.com/zhaopin/Python/?labelWords=label")
response = urlopen(request)

# 如果直接访问拉勾网， 这是没有任何有价值的内容的。
# html = response.read().decode("utf-8")
# print(html)

bs = BeautifulSoup(response, "html.parser")
district_list = bs.select("span.add em")
# print(len(district_list))

for district in district_list:
    print(district.text)


def get_lagou_all_link():
    url = "https://www.lagou.com/"
    lagou_response = urlopen(url)
    lagou_bs = BeautifulSoup(lagou_response, "html.parser")
    lagou_list = lagou_bs.select("div.mainNavs a")
    print("----", lagou_list)
    result_list  = []


get_lagou_all_link()
