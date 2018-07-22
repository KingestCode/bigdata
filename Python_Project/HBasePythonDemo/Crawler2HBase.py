#!/usr/bin/python3
# -*-coding:utf-8-*-


# 首先创建 hbase 表: pages
# $hbase> create 'ns1:pages','f1'

import urllib.request
import os
import re
import CrawerPageDao

#下载网页方法
def download(url):
    #判断当前的网页是否已经下载
    resp = urllib.request.urlopen(url)
    pageBytes = resp.read()
    resp.close

    if not CrawerPageDao.exists(url):
        CrawerPageDao.savePage(url, pageBytes);

    try:
        #解析网页的内容
        pageStr = pageBytes.decode("utf-8");
        #解析href地址
        pattern = u'<a[\u0000-\uffff&&^[href]]*href="([\u0000-\uffff&&^"]*?)"'
        res = re.finditer(pattern, pageStr)
        for r in res:
            addr = r.group(1);
            print(addr)
            if addr.startswith("//"):
                addr = addr.replace("//","http://");

            #判断网页中是否包含自己的地址
            if addr.startswith("http://") and url != addr and (not CrawerPageDao.exists(addr)):
                download(addr) ;

    except Exception as e:
        print(e)
        print(pageBytes.decode("gbk", errors='ignore'));
        return ;

download("http://jd.com");