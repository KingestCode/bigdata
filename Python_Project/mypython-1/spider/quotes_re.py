from urllib.request import urlopen
from urllib.request import urlretrieve

url = "http://quotes.toscrape.com/"
response = urlopen(url)
html = response.read().decode("UTF-8")
# print(html)

# 把返回回来的服务器html保存为一个本地文件
urlretrieve(url, "/Users/shixuanji/Documents/Code/MySpider/quotes/quotes.html")

import  re

quote_list = re.findall('<span class="text" itemprop="text">(.*)</span>',html)
# print(result_list)
# print(type(result_list))
# print(len(result_list))

for q in quote_list:
    print(q.strip("“”"))


author_list = re.findall('<small class="author" itemprop="author">(.*)</small>',html)

for a in author_list:
    print(a.strip("“”"))

# 抓取标签
# 直接抓取meta信息抓取不到，放弃， 抓取其下的 a 标签中的内容
# tags = re.findall('<meta class="keywords" itemprop="keywords" content="(.*)">', html)
# 这种情况，换行符通过.*的方式是匹配不出来的。
# tags = re.findall('<div class="tags">(.*)</div>', html)

# .*后加上? 就是非贪婪模式
tags = re.findall('<div class="tags">(.*?)</div>',html,re.RegexFlag.DOTALL)

print(len(tags))

true_tags = []
for tag in tags:
    # print(tag)
    atag = re.findall('<a class="tag" href=".*">(.*)</a>', tag)
    true_tags.append(atag)

# print(true_tags)

last = []

# 整合最终的结果
for index in range(len(quote_list)):
    q = quote_list[index].strip("“”")
    a = author_list[index]
    t = ",".join(true_tags[index])
    print(q,a,t,sep="\t\t")
