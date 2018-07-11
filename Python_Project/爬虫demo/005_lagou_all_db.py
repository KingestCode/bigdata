#!/usr/bin/python3
# -*- coding: utf-8 -*-


# ============================================================
# 内容描述： 拉勾应用
# ============================================================

from urllib.request import urlopen
from urllib.request import Request
from bs4 import BeautifulSoup
import time
import pymysql


"""

数据库建表语句：

create database if not exists spider;
use spider;
CREATE TABLE if not exists `lagou` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `t_job` varchar(255) DEFAULT NULL,
  `t_addr` varchar(255) DEFAULT NULL,
  `t_tag` varchar(255) DEFAULT NULL,
  `t_com` varchar(255) DEFAULT NULL,
  `t_money` varchar(255) DEFAULT NULL,
  `t_edu` varchar(255) DEFAULT NULL,
  `t_exp` varchar(255) DEFAULT NULL,
  `t_type` varchar(255) DEFAULT NULL,
  `t_level` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
show tables;
select * from lagou;

"""


# 模拟了使用浏览器在访问
head = {"User-Agent":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36",
        "Cookie":"_ga=GA1.2.572475695.1522203171; user_trace_token=20180328101310-8faca567-322d-11e8-a23e-525400f775ce; LGSID=20180328101310-8faca6f5-322d-11e8-a23e-525400f775ce; PRE_UTM=; PRE_HOST=; PRE_SITE=; PRE_LAND=https%3A%2F%2Fwww.lagou.com%2F; LGUID=20180328101310-8faca856-322d-11e8-a23e-525400f775ce; _gid=GA1.2.917932989.1522203171; index_location_city=%E5%8C%97%E4%BA%AC; JSESSIONID=ABAAABAAAIAACBICD5501D7E1D8273C03AFB6704807496B; TG-TRACK-CODE=index_navigation; Hm_lvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1522203691; X_HTTP_TOKEN=483c5dc38a3823a6b6b0f4fee4873734; _gat=1; SEARCH_ID=beab97953e78487eb14fe25a9ed9e141; LGRID=20180328102808-a7093390-322f-11e8-b652-5254005c3644; Hm_lpvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1522204069"}


con = pymysql.connect(host="hadoop02", user="root", password="root", database="spider", charset='utf8', port=3306)
cursor = con.cursor()

# [jobs[i],addrs[i],companys[i],true_tags[i],moneys[i],edus[i],exps[i]]
lagou_insert_sql = "insert into lagou (t_job, t_addr, t_com, t_tag, t_money, t_edu, t_exp, t_type, t_level) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"


# 获取所有职业类型的URL
def get_all_url():
    url = "https://www.lagou.com/"
    request1 = Request(url, headers=head)
    response = urlopen(request1)
    bs = BeautifulSoup(response, "html.parser")
    url_list_a = bs.select("div.menu_sub a")
    url_list = [url.get("href") for url in url_list_a]

    # url_list.remove("https://www.lagou.com/zhaopin/C%23/")
    # print(len(url_list))
    # print(url_list)

    return url_list




'''
https://www.lagou.com/zhaopin/Java/
'''
def crawl(counter, link):

    print("正在爬取第 %s 个URL ： %s" %(counter,link))
    # link = "https://www.lagou.com/zhaopin/C%23/"

    page = 1
    # 获取这个link锁对应的所有页数
    while True:
        time.sleep(1)

        # 当前的这个URL会是涉及分页的。
        new_link = link + str(page) + "/"

        request = Request(new_link, headers=head)
        temp = 1
        try:
            url_response = urlopen(request)
            url_bs = BeautifulSoup(url_response, "html.parser")
        except:
            temp = 2
        else:

            #  正常的逻辑处理

            job_list = url_bs.select("ul.item_con_list li h3")
            # 工作岗位  jobs
            jobs = [job_h3.text for job_h3 in job_list]
            if len(jobs) == 0:
                break

            # 岗位地点  addrs
            addrs = url_bs.select("span.add em")
            addrs = [addr.text for addr in addrs]

            # 公司  companys
            companys = url_bs.select("div.company_name a")
            companys = [c.text for c in companys]

            # 职位标签  true_tags
            tagss = url_bs.select("div.list_item_bot div.li_b_l")
            true_tags = []
            for tags in tagss:
                spans = tags.select("span")
                tag_content = ",".join([span.text for span in spans])
                true_tags.append(tag_content)

            # 职位薪资  moneys
            moneys = url_bs.select("span.money")
            moneys = [money.text for money in moneys]

            # 经验要求 exps  和学历要求   edus
            exps_and_edus = url_bs.select("div.p_bot div.li_b_l")
            exps_and_edus = [ee.text.strip() for ee in exps_and_edus]
            edus = [edu.split(" / ")[1] for edu in exps_and_edus]
            expss = [edu.split(" / ")[0] for edu in exps_and_edus]
            exps = [exp.split("\n")[1] for exp in expss]

            # 公司描述  types  和  公司融资级别  levels
            types_and_level = url_bs.select("div.industry")
            tl = [tal.text.strip() for tal in types_and_level]
            types = []
            levels = []
            for tals in tl:
                t_a_l = tals.split(" / ")
                if len(t_a_l) != 2:
                    types.append("无")
                    levels.append("无")
                else:
                    types.append(t_a_l[0])
                    levels.append(t_a_l[1])

            print("     爬取第  %d 页成功" % page)

            ##  取出的值： jobs    addrs    companys     true_tags     moneys     edus   exps    types   levels
            for i in range(len(jobs)):
                record = [jobs[i],addrs[i],companys[i],true_tags[i],moneys[i],edus[i],exps[i],types[i],levels[i]]
                # print("\t".join(record))
                cursor.execute(lagou_insert_sql, record)

            con.commit()

            page += 1
        finally:
            if temp == 2:
                break




# 遍历所有职业类型的URL 来获取这个职业类型中的所有 招聘信息

if __name__ == '__main__':
    link_list = get_all_url()
    counter = 0
    for link in link_list:
        counter += 1
        crawl(counter,link)

    cursor.close()
    con.close()