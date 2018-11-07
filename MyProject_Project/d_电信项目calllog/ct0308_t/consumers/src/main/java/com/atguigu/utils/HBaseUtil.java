package com.atguigu.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * 1.创建命名空间
 * 2.判断表是否存在
 * 3.创建表
 * 4.生成rowkey
 * 5.预分区健的生成
 */
public class HBaseUtil {

    private static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
    }

    //1.创建命名空间
    public static void createNamespace(String nameSpace) throws IOException {

        //获取连接对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //获取命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        //创建命名空间
        admin.createNamespace(namespaceDescriptor);
        //关闭资源
        admin.close();
        connection.close();
    }

    //2.判断表是否存在
    public static boolean existTable(String tableName) throws IOException {
        //获取连接对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //判断
        boolean tableExists = admin.tableExists(TableName.valueOf(tableName));
        //关闭资源
        admin.close();
        connection.close();

        return tableExists;
    }

    //3.创建表
    public static void createTable(String tableName, int regions, String... cfs) throws IOException {
        //获取连接对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //判断表是否存在
        if (existTable(tableName)) {
            System.out.println("表：" + tableName + "已存在！");
            //关闭资源
            admin.close();
            connection.close();
            return;
        }
        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //循环添加列族
        for (String cf : cfs) {
            //创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
//        hTableDescriptor.addCoprocessor("com.atguigu.coprocessor.MyCoprocessor");
        //创建表
        admin.createTable(hTableDescriptor, getSplitKeys(regions));
        //关闭资源
        admin.close();
        connection.close();
    }

    //预分区健的生成
    //00|,01|,02|,03|,04|,05|
    private static byte[][] getSplitKeys(int regions) {

        //创建分区健二维数据
        byte[][] splitKeys = new byte[regions][];
        DecimalFormat df = new DecimalFormat("00");

        //循环添加分区健
        for (int i = 0; i < regions; i++) {
            splitKeys[i] = Bytes.toBytes(df.format(i) + "|");
        }

        return splitKeys;
    }

    //生成rowkey
    //0x_13712341234_2017-05-02 12:23:55_时间戳_13598769876_duration
    public static String getRowKey(String rowHash, String caller, String buildTime, String buildTS, String callee, String flag, String duration) {

        return rowHash + "_"
                + caller + "_"
                + buildTime + "_"
                + buildTS + "_"
                + callee + "_"
                + flag + "_"
                + duration;
    }

    //生成分区号（13712341234，2017-05-02 12:23:55）
    // HBASE执行流程
        //首先在创建表的时候, 添加进预分区键, 预分区键用 0x, 或者 xx 都可以, 是自己定义的
        //在设计 rowkey 的时候, 要同时考虑到离散和聚合, 离散是为了避免热点, 聚合是为了业务需要, 比如业务要统计每天,月,年订单, 此时如果按照年来聚合,数据量会过大, 此时可以按照月聚合, 因此把每一天数据, 按照用户名(后台会有用户名对应 id)取出id 的一定位数特征, 与 年月 取 ^(抑或), 然后再模以 region 数, 就可以得到一个 0x 的数, 会落到一个分区中, 此用户在一个月内的订单数据都会落到一个 region 中
    public static String getRowHash(int regions, String caller, String buildTime) {

        DecimalFormat df = new DecimalFormat("00");

        //取手机号中间4位
        String phoneMid = caller.substring(3, 7);
        String yearMonth = buildTime.replace("-", "").substring(0, 6);

        int i = (Integer.valueOf(phoneMid) ^ Integer.valueOf(yearMonth)) % regions;

        return df.format(i);
    }

}
