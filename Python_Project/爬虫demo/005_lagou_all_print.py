#!/usr/bin/python3
# -*- coding: utf-8 -*-


# ============================================================
# 内容描述： 拉勾应用
# ============================================================

from urllib.request import urlopen
from urllib.request import Request
from bs4 import BeautifulSoup
import time

head = {"User-Agent":'Mozilla/5.0',
       "Cookie":"user_trace_token=20180103182645-99459851-f070-11e7-9fc4-5254005c3644; LGUID=20180103182645-9945a1b3-f070-11e7-9fc4-5254005c3644; WEBTJ-ID=20180327134408-16265fc301594-0c66caddeff5b9-6e11107f-3686400-16265fc30166a7; JSESSIONID=ABAAABAAAGFABEF733D37AB2482DBC67214869650457103; index_location_city=%E5%8C%97%E4%BA%AC; TG-TRACK-CODE=index_navigation; _gat=1; PRE_UTM=; PRE_HOST=; PRE_SITE=; PRE_LAND=https%3A%2F%2Fwww.lagou.com%2Fzhaopin%2FPython%2F%3FlabelWords%3Dlabel; SEARCH_ID=fe9635d3a0e845c7ac4b332b0f75efe1; _gid=GA1.2.1532909077.1522129430; Hm_lvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1522129465,1522153423,1522153427,1522153940; Hm_lpvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1522156310; _ga=GA1.2.1903431618.1514975204; LGSID=20180327211140-6334ffee-31c0-11e8-b64e-5254005c3644; LGRID=20180327211149-686ce6df-31c0-11e8-a168-525400f775ce"}

def get_lagou_all_link():
    url = "https://www.lagou.com/"
    request = Request(url, headers=head)
    lagou_response = urlopen(request)
    lagou_bs = BeautifulSoup(lagou_response, "html.parser")
    lagou_list = lagou_bs.select("div.mainNavs a")
    url_list = [link.get("href") for link in  lagou_list]
    url_list = set(url_list)
    return url_list

url_list = get_lagou_all_link()
print(url_list)
print(len(url_list))

def crawl(link):
    for page in range(1, 31):
        url = link + str(page) + "/"
        print("即将抓取第%d页数据，url为：%s"%(page, url))
        request = Request(url, headers=head)
        response = urlopen(request)
        # 如果请求的url不等于服务器回应的url，说明本页已经没有数据，返回，
        # 爬取下一个招聘岗位的链接。
        if url != response.geturl():
            print("第%d页没有数据，继续抓取下一个链接。"%page)
            return
        soup = BeautifulSoup(response, "html.parser")
        title = soup.select("a.position_link h3")
        loc = soup.select("span.add em")
        s_e_e = soup.select("div.p_bot > div.li_b_l")
        tags = soup.select("div.list_item_bot > div.li_b_l")
        company = soup.select("div.company_name > a")
        d_s = soup.select("div.industry")
        adv = soup.select("div.li_b_r")

        for i in range(len(title)):
            _title = title[i].text
            _loc = loc[i].text
            temp = list(s_e_e[i].stripped_strings)
            _salary = temp[0]
            # split 对参数指定的内容进行切割，返回切割之后的列表。
            temp = temp[1].split(" / ")
            _exp = temp[0]
            _edu = temp[1]
            _tags = ",".join(tags[i].stripped_strings)
            _company = company[i].text
            temp = d_s[i].text.strip()
            temp = temp.split(" / ")
            if len(temp) == 2:
                _domain = temp[0]
                _stage = temp[1]
            else:
                _domain = "无"
                _stage = "无"
            _adv = adv[i].text
            print([_title, _loc, _salary, _exp, _edu, _tags, _company, _domain, _stage, _adv])
        # 令当前程序暂停参数指定的时间。单位：秒。支持小数。
        time.sleep(1)


for job_link in url_list:
    crawl(job_link)