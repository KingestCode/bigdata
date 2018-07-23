#!/usr/bin/python3
# -*-coding:utf-8-*-

from urllib.request import urlopen
from urllib.request import Request
from bs4 import BeautifulSoup
import time
import pymysql

# 模拟使用浏览器访问
header = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:61.0) Gecko/20100101 Firefox/61.0",
    "Cookie": "JSESSIONID=ABAAABAAAIAACBI9635EDB9DA7E767CDCD9587B4D72583B; _ga=GA1.2.1617665615.1530873944; index_location_city=%E4%B8%8A%E6%B5%B7; user_trace_token=20180706184547-bdb64b0a-8109-11e8-98e3-5254005c3644; LGSID=20180706184547-bdb64cf6-8109-11e8-98e3-5254005c3644; LGRID=20180706193751-0406f0d0-8111-11e8-98e3-5254005c3644; LGUID=20180706184547-bdb64f8b-8109-11e8-98e3-5254005c3644; TG-TRACK-CODE=search_code; SEARCH_ID=7302b5ebd50841ad92543e9e2161bd9a; _gat=1"
}


# 连接 mysql


# 获取所有职业类型的 url

def get_all_url():
    url = "https://www.lagou.com/"
    request1 = Request(url, headers=header)
    response = urlopen(request1)
    bs = BeautifulSoup(response, "html.parser")
    url_list_a = bs.select("div.menu_sub a")
    url_list = [url.get("href") for url in url_list_a]
    return url_list


'''
link: https://www.lagou.com/zhaopin/Hadoop
'''


def crawl(link):

    # 获取这个 link对应的所有页数
    for page in range(1,31):

        # 分页, 其实就是拼接页码数
        url = link + str(page) + "/"
        print("即将抓取第%d页数据，url为：%s"%(page, url))

        # 爬取指定 页数 & 网址 页面
        request = Request(url, headers=header)
        response = urlopen(request)
        # 如果请求的url不等于服务器回应的url，说明本页已经没有数据，返回，
        # 爬取下一个招聘岗位的链接。
        respUrl = response.geturl()
        if url != respUrl:
            print("当前--拼接--的 url 为: %s"%url)
            print("当前--返回--的 url 为: ----->>>%s"%respUrl)
            print("第%d页没有数据，继续抓取下一个链接。"%page)
            return

        url_bs = BeautifulSoup(response, "html.parser")

        ### 正常处理页面的逻辑
        #==================== 拿到总页面数div.page-number span.span
        # totalNum = url_bs.select("div.page-number span.span")
        # tnum = [tnum.text for tnum in totalNum]
        #=====================好像用不上..

        # 工作岗位
        job_list = url_bs.select("ul.item_con_list li h3")
        # print(len(job_list))
        jobs = [job_h3.text.strip() for job_h3 in job_list]
        if len(jobs) == 0:
            print("当前 job 名为空")
            print(url)
            time.sleep(5)

        ## 公司名 company
        companys_list = url_bs.select("div.company_name a")
        companys = [companys_a.text.strip() for companys_a in companys_list]

        ## 公司地址 addr
        addrs = url_bs.select("span.add em")
        addrs = [addr.text.strip() for addr in addrs]

        ## 薪资 salarys
        moneys_list = url_bs.select("span.money")
        moneys = [moneys.text.strip() for moneys in moneys_list]

        ## 经验/学历 要求 exps
        exps_list = url_bs.select("div.p_bot div.li_b_l")
        exps_and_edus = [exps.text.strip() for exps in exps_list]
        expss = [exps.split("/")[0] for exps in exps_and_edus]

        # 学历
        edus = [exps.split("/")[1].strip() for exps in exps_and_edus]
        # 经验
        exps = [exps.split("\n")[1].strip() for exps in expss]

        ## 公司类型/融资状况 cmptype_and_financing
        cmptype_and_financing_list = url_bs.select("div.industry")
        # 公司类型
        cmptypes = [c_n_f.text.split("/")[0].strip() for c_n_f in cmptype_and_financing_list]
        # 融资状态
        fincs = [c_n_f.text.split("/")[1].strip() for c_n_f in cmptype_and_financing_list]
        # for i in range(len(companys)):
        #     print(companys[i] + "," + fincs[i])

        # job 标签 tags
        job_tagss = url_bs.select("div.list_item_bot div.li_b_l")
        true_tags = []
        for tags in job_tagss:
            spans = tags.select("span")
            # print(spans)
            tag_content = ",".join([span.text.strip() for span in spans])  # type=string
            true_tags.append(tag_content)
            # for t_tag in true_tags:
            #     print(t_tag)
            #     print("---")

        # 公司优势  company_brightes
        company_brightes = url_bs.select("div.list_item_bot div.li_b_r")
        c_b_s = [company_brighte.text.strip("“”") for company_brighte in company_brightes]

        ###############打印所有的看看
        """
        jobs # 工作岗位
        companys #公司名
        addrs	#公司地址
        moneys # 薪资
        edus # 学历
        exps # 经验
        cmptypes # 公司类型
        fincs  # 融资状态
        true_tags #job 标签
        c_b_s  # 公司优势
        """

        for i in range(len(jobs)):
            record = [jobs[i], addrs[i], companys[i], moneys[i], edus[i], exps[i], cmptypes[i], fincs[i],
                      true_tags[i], c_b_s[i]]
            print(record)
            print("--------------------------")




#======================================
# 遍历所有职业类型的 URL, 来获取这个职业类型中的所有招聘信息
#======================================


link_list = get_all_url()

for link in link_list:
    crawl(link)
    time.sleep(5)









