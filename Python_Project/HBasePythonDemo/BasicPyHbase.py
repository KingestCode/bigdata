# -*- encoding=utf-8 -*-

import os

#导入thrift的python模块
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

#导入自已编译生成的hbase python模块
from mythrift.hbase import THBaseService
from mythrift.hbase.ttypes import *
from mythrift.hbase.ttypes import TResult


#创建Socket连接，到s201:9090
transport = TSocket.TSocket('cs1', 9090)
transport = TTransport.TBufferedTransport(transport)
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = THBaseService.Client(protocol)

#打开传输端口!!!
transport.open()

## put操作
# table = b'ns1:t1'
# row = b'row1'
# v1 = TColumnValue(b'f1', b'id', b'101')
# v2 = TColumnValue(b'f1', b'name', b'tomas')
# v3 = TColumnValue(b'f1', b'age', b'12')
# vals = [v1, v2, v3]
# put = TPut(row, vals)
# client.put(table, put)
# print("okkkk!!")
# transport.close()


# #get
# table = b'ns1:t1'
# rowkey=b"row1"
# col_id = TColumn(b"f1",b"id")
# col_name = TColumn(b"f1",b"name")
# col_age = TColumn(b"f1",b"age")
#
# cols = [col_id,col_name,col_age]
# get = TGet(rowkey,cols)
# res = client.get(table,get)
# print(bytes.decode(res.columnValues[0].qualifier))
# print(bytes.decode(res.columnValues[0].family))
# print(res.columnValues[0].timestamp)
# print(bytes.decode(res.columnValues[0].value))


# #delete
# table = b'ns1:t1'
# rowkey = b"row1"
# col_id = TColumn(b"f1", b"id")
# col_name = TColumn(b"f1", b"name")
# col_age = TColumn(b"f1", b"age")
# cols = [col_id, col_name]
#
# #构造删除对象
# delete = TDelete(rowkey,cols)
# res = client.deleteSingle(table, delete)
# transport.close()
# print("ok")


# Scan
table = b'ns1:t12'
startRow = b'1530357094900-43dwMLjxI5-0'
stopRow = b'1530357183537-43dwMLjxI5-6'
payload = TColumn(b"f1", b"payload")

cols = [payload]

scan = TScan(startRow=startRow,stopRow=stopRow,columns=cols)
# 这里如果不传 stopRow 就是扫描到结尾
scan = TScan(startRow=startRow, columns=cols)
r = client.getScannerResults(table,scan,100);
for x in r:
    print("============")
    print(bytes.decode(x.columnValues[0].qualifier))
    print(bytes.decode(x.columnValues[0].family))
    print(x.columnValues[0].timestamp)
    print(bytes.decode(x.columnValues[0].value))