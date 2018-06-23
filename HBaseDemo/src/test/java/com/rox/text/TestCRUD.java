package com.rox.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;


public class TestCRUD {

    @Test
    public void put() throws IOException {
        // 创建 conf 对象
        Configuration conf = HBaseConfiguration.create();/* 通过连接工厂创建连接对 */

        // 通过连接工厂创建连接对象
        Connection conn = ConnectionFactory.createConnection(conf);

        // 通过连接查询 TableName 对象
        TableName tname = TableName.valueOf("ns1:t1");

        // 获得 table对象
        Table table = conn.getTable(tname);

        // 通过bytes 工具类转化字符串为字节数组
        byte[] bytes = Bytes.toBytes("row3");

        // 创建 put 对象, 传入行号
        Put put = new Put(bytes);

        // 创建 列族, 列, value 的 byte 数据
        byte[] f1 = Bytes.toBytes("f1");
        byte[] id = Bytes.toBytes("id");
        byte[] value = Bytes.toBytes(188);
        put.addColumn(f1, id, value);

        // table put 数据
        table.put(put);

        //============================

        // get
        byte[] rowid = Bytes.toBytes("row3");
        Get get = new Get(rowid);

        Result res = table.get(get);
        byte[] idvalue = res.getValue(Bytes.toBytes("f1"),Bytes.toBytes("id"));
        System.out.println(Bytes.toInt(idvalue));

    }

}
