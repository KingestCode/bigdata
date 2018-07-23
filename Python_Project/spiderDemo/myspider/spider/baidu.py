from urllib.request import urlopen

# 发起请求的URL地址
url = "http://www.baidu.com/"

# 发起请求，请求百度，获取网络请求返回的相应，使用response对象接收
response = urlopen(url)

# 获取服务器进行响应的url地址，服务器返回的url不一定是我们使用的url，有可能是服务器内部发生了跳转的url地址
print(response.geturl())
# 获取服务器返回的元信息， 在http协议和HTTPS中，返回的就是 headers 信息
print(response.info())
# 返回服务器给我们返回的请求状态码
# 一般来说，常用的http协议返回状态码：
# 200 表示请求成功
# 403 请求已经被服务器接收，但是拒绝处理
# 404 表示请求资源未找到
# 500 表示服务器内部错误
print(response.getcode())

html = response.read()
# 获取到了html内容之后， 获取的是字节形式，如果遇到多字节的字符，就无法显示了。所以需要进行解码
# response对象中的decode方法可以对html内容进行解码。解码的时候可以指定对应的字符编码集
html = html.decode("UTF-8")
print(html)


