package com.rox.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import sun.tools.jconsole.Tab;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;


public class TestCRUD {

    /**
     * put
     * @throws IOException
     */
    @Test
    public void put() throws IOException {
        // 创建 conf 对象
        Configuration conf = HBaseConfiguration.create();

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
    }


    /**
     * get
     * @throws IOException
     */
    @Test
    public void get() throws IOException {

        Configuration conf = HBaseConfiguration.create();

        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tname = TableName.valueOf("ns1:t1");
        Table table = conn.getTable(tname);

        Get get = new Get(Bytes.toBytes("row3"));

        Result res = table.get(get);
        byte[] idvalue = res.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
        System.out.println(Bytes.toInt(idvalue));

    }



    /**
     * 百万数据的插入 --- bigPUT
    * @throws IOException
     */
    @Test
    public void insertMillion() throws IOException {

        long start = System.currentTimeMillis();

        DecimalFormat format = new DecimalFormat();
        format.applyPattern("000000");

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");
        HTable table = (HTable) conn.getTable(tname);
        // 设置不自动清理缓冲区, 注意: 版本为1.2.6
        table.setAutoFlush(false);

        for (int i = 0; i < 1000; i++) {
            Put put = new Put(Bytes.toBytes("row" + format.format(i)));
//            Put put = new Put(Bytes.toBytes("row" + i));
            // 1.2.6版本的 关闭写前日志
            put.setWriteToWAL(false);

            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("id"), Bytes.toBytes(i));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("tom" +i));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"), Bytes.toBytes(i % 100));
            table.put(put);

            if (i % 500 == 0) {
                table.flushCommits();
            }
        }
        table.flushCommits();
        System.out.println(System.currentTimeMillis() - start);
    }


    /**
     * createNameSpace
     * 创建命名空间
     * @throws Exception
     */
    @Test
    public void createNameSpace() throws Exception {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        // 创建命名空间描述符
        NamespaceDescriptor nsd = NamespaceDescriptor.create("ns2").build();
        // 再用 admin 创建命名空间
        admin.createNamespace(nsd);

        NamespaceDescriptor[] ns = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor n: ns) {
            System.out.println(n.getName());
        }
    }


    /**
     * listNameSpace
     * 展示 命名空间
     * @throws Exception
     */
    @Test
    public void listNameSpace() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        NamespaceDescriptor[] ns = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor n: ns) {
            System.out.println(n.getName());
        }
    }


    /**
     * 创建表
     * @throws Exception
     */
    @Test
    public void createTable() throws Exception {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        // 通过连接获取管理类
        Admin admin = conn.getAdmin();

        //创建表名对象
        TableName tname = TableName.valueOf("ns2:t2");

        //创建表描述符对象
        HTableDescriptor tbl = new HTableDescriptor(tname);
        // 创建列族描述符
        HColumnDescriptor col = new HColumnDescriptor("f1");

        // 表描述符对象 添加 列族描述符对象
        tbl.addFamily(col);

        //管理对象创建表(表描述符)
        admin.createTable(tbl);

        // 创建完毕
        System.out.println("over");
    }


    /**
     * 删除表
     * @throws Exception
     */
    @Test
    public void disableTable() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("ns2:t2");
        // 禁用表 disable... 启用表 enable...
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }


    /**
     * 删除指定行中,某列数据, 或者直接删除某一行
     * @throws Exception
     */
    @Test
    public void deleteData() throws Exception {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tname = TableName.valueOf("ns1:t1");
        Table table = conn.getTable(tname);

        Delete delete = new Delete(Bytes.toBytes("row000998"));
        delete.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("id"));
        // 如果delete 中只添加了行号, 则是删除这一整行
        // 如果  addColumn(),  添加了 哪一列, 就删除哪一列
        table.delete(delete);
        System.out.println("over");
    }


    /**
     * scan 扫描
     * 查询某一列的数据
     * @throws Exception
     */
    @Test
    public void scan() throws Exception {

        Configuration conf = HBaseConfiguration.create();

        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tname = TableName.valueOf("ns1:t1");
        Table table = conn.getTable(tname);

        // 创建 Scan 对象
        Scan scan = new Scan();
        // 设置起始位置
        scan.setStartRow(Bytes.toBytes("row000100"));
        scan.setStopRow(Bytes.toBytes("row000111"));

        // table 对象获取 scan 对象, 得到扫描结果
        ResultScanner rs = table.getScanner(scan);
        // 迭代
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result rss = it.next();
            byte[] name = rss.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.out.println(Bytes.toString(name));
        }
    }


    /**
     * 动态遍历
     * 指定列族下的所有 map
     * 遍历出此列族中指定起始位置的所有列的值
     * @throws Exception
     */
    @Test
    public void scan2() throws Exception {
        Configuration conf = HBaseConfiguration.create();

        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tname = TableName.valueOf("ns1:t1");
        Table table = conn.getTable(tname);

        // 创建 Scan 对象
        Scan scan = new Scan();
        // 设置起始位置
        scan.setStartRow(Bytes.toBytes("row000100"));
        scan.setStopRow(Bytes.toBytes("row000111"));

        // table 对象获取 scan 对象, 得到扫描结果
        ResultScanner rs = table.getScanner(scan);
        // 迭代
        Iterator<Result> it = rs.iterator();

        while (it.hasNext()) {
            Result r = it.next();
            // 得到 指定列族下的所有 map
            Map<byte[], byte[]> f1Map = r.getFamilyMap(Bytes.toBytes("f1"));
            for (Map.Entry<byte[], byte[]> entrySet : f1Map.entrySet()) {
                String col = Bytes.toString(entrySet.getKey());
                String val = Bytes.toString(entrySet.getValue());
                System.out.println(col + ":" + val + ",");
            }
            System.out.println();
        }
        /**
         * 执行结果
         * age:    ,
         id:   d,
         name:tom100,

         age:   ,
         id:   e,
         name:tom101,
         */
    }


    /**
     * 动态遍历
     * 所有的表中指定起始rowkey位置,
     * 的所有 列族 & 列 & value
     * @throws Exception
     */
    @Test
    public void scan3() throws Exception {
        Configuration conf = HBaseConfiguration.create();

        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tname = TableName.valueOf("ns1:t1");
        Table table = conn.getTable(tname);

        // 创建 Scan 对象
        Scan scan = new Scan();
        // 设置起始位置
        scan.setStartRow(Bytes.toBytes("row000100"));
        scan.setStopRow(Bytes.toBytes("row000111"));

        // table 对象获取 scan 对象, 得到扫描结果
        ResultScanner rs = table.getScanner(scan);
        // 迭代
        Iterator<Result> it = rs.iterator();

        while (it.hasNext()) {
            Result r = it.next();
            // 得到 指定列族下的所有 map
            // 格式:  key=f1,value=Map<Col,Map<Timestamp,value>>
            Map<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = r.getMap();

            for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry :
                    map.entrySet()) {

                // 拿到列族
                String cf = Bytes.toString(entry.getKey());

                // Map<Col,Map<Timestamp,value>>
                Map<byte[], NavigableMap<Long, byte[]>> colDataMap = entry.getValue();

                // 继续遍历
                for (Map.Entry<byte[], NavigableMap<Long, byte[]>> ets : colDataMap.entrySet()
                        ) {
                    String c = Bytes.toString(ets.getKey());
                    Map<Long, byte[]> tsValue = ets.getValue();
                    for (Map.Entry<Long, byte[]> e : tsValue.entrySet()) {
                        Long ts = e.getKey();
                        String value = Bytes.toString(e.getValue());
                        System.out.println(cf + ":" + c + ":" + ts + "=" + value + ",");
                    }
                }
            }
            System.out.println();
        }
    }





}


