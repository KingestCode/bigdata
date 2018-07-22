#!/usr/bin/python3
# -*- coding:utf-8 -*-
# author: https://blog.csdn.net/zhongqi2513

# ============================================================
# 内容描述： 拉勾应用
# ============================================================

from urllib.request import urlopen
from urllib.request import Request
from bs4 import BeautifulSoup



from urllib.request import urlopen
from urllib.request import Request
from bs4 import BeautifulSoup

url = "https://www.lagou.com/zhaopin/Python/?labelWords=label"

head = {"User-Agent":'Mozilla/5.0',
       "Cookie":"JSESSIONID=ABAAABAAADEAAFI2F5F2E04D14F97DF8823A1B5789349DB; _ga=GA1.2.313563134.1530865167; _gat=1; user_trace_token=20180706162000-60380ac8-80f5-11e8-bf23-525400f775ce; LGSID=20180706162000-60380c7a-80f5-11e8-bf23-525400f775ce; PRE_UTM=; PRE_HOST=; PRE_SITE=; PRE_LAND=https%3A%2F%2Fwww.lagou.com%2F; LGRID=20180706162000-60380dc9-80f5-11e8-bf23-525400f775ce; LGUID=20180706162000-60380e55-80f5-11e8-bf23-525400f775ce; index_location_city=%E5%8C%97%E4%BA%AC"}
request = Request(url, headers=head)

response = urlopen(request)

bs = BeautifulSoup(response, "html.parser")

district_list = bs.select("span.add em")
# print(len(district_list))

for district in district_list:
    print(district.text)


#################  其它的标签待写 ####################


def get_lagou_all_link():
    url = "https://www.lagou.com/"
    lagou_response = urlopen(url)
    lagou_bs = BeautifulSoup(lagou_response, "html.parser")
    lagou_list = lagou_bs.select("div.mainNavs a")
    print("----", lagou_list)
    result_list  = []



get_lagou_all_link()


