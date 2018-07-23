#!/usr/bin/python3
# -*-coding:utf-8-*-

# ====================================================
# 内容描述： 获取到源码内容之后可以保存到本地，也可以下载图片等数据
# ====================================================

# 发起网络请求，获得服务器返回给我们的内容。
# 从urllib包的request模块中引入urlopen函数。
from urllib.request import urlopen
from urllib.request import urlretrieve


# 发起请求的URL地址
url = "http://www.baidu.com/"

# 发起请求，请求百度，获取网络请求返回的相应，使用response对象接收
response = urlopen(url)

html = response.read()
# 获取到了html内容之后， 进行解码
html = html.decode("UTF-8")
# print(html)

# 把返回回来的服务器html保存为一个本地文件
urlretrieve(url, "/Users/shixuanji/Documents/Code/MySpider/baidu/baidu.html")


# 检查结果发现，首页中的图片不显示： 那么如何下载图片呢。
urlretrieve("http://www.baidu.com/img/bd_logo1.png", "/Users/shixuanji/Documents/Code/MySpider/baidu/bd_logo1.png")
