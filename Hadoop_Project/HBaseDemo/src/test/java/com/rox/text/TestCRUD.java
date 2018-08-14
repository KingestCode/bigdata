package com.rox.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;


public class TestCRUD {

    /**
     * put
     *
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
     *
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
     *
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
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("tom" + i));
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
     *
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
        for (NamespaceDescriptor n : ns) {
            System.out.println(n.getName());
        }
    }


    /**
     * listNameSpace
     * 展示 命名空间
     *
     * @throws Exception
     */
    @Test
    public void listNameSpace() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        NamespaceDescriptor[] ns = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor n : ns) {
            System.out.println(n.getName());
        }
    }


    /**
     * 创建表
     *
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
     *
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
     *
     * @throws Exception
     */
    @Test
    public void deleteData() throws Exception {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tname = TableName.valueOf("ns1:t1");
        Table table = conn.getTable(tname);

        Delete delete = new Delete(Bytes.toBytes("row000998"));
        delete.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("id"));
        // 如果delete 中只添加了行号, 则是删除这一整行
        // 如果  addColumn(),  添加了 哪一列, 就删除哪一列
        table.delete(delete);
        System.out.println("over");
    }


    /**
     * scan 扫描
     * 查询某一列的数据
     *
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
     *
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
     *
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
        /**
         * 执行结果
         * f1:age:1529982061730=    ,
         f1:id:1529982061730=   d,
         f1:name:1529982061730=tom100,

         f1:age:1529982061730=   ,
         f1:id:1529982061730=   e,
         f1:name:1529982061730=tom101,
         */
    }


    /**
     * 按照指定的版本数查询数据
     *
     * @throws Exception
     */
    @Test
    public void getWithVersions() throws Exception {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t3");
        Table table = conn.getTable(tname);

        // 创建 get 对象
        Get get = new Get(Bytes.toBytes("row1"));

        // 检索所有版本,注意, 这里是 setMaxVersions(), 不是 get
        // 这里就是拿到 create 的时候, 指定version 的最大值
        get.setMaxVersions();

        // 这里可以设置时间戳, [) : 区间是前包后不包
        get.setTimeRange(Long.parseLong("1530019540164"), Long.parseLong("1530019546710"));

        // table 对象 通过 get 对象拿到结果
        Result r = table.get(get);

        // 在结果中获取指定行的指定 列族 和 列的所有数据
        List<Cell> columnCells = r.getColumnCells(Bytes.toBytes("f1"), Bytes.toBytes("name"));
        for (Cell c : columnCells) {
            String f = Bytes.toString(c.getFamily());
            String col = Bytes.toString(c.getQualifier());
            Long ts = c.getTimestamp();
            String val = Bytes.toString(c.getValue());
            System.out.println(f + "/" + col + "/" + ts + "=" + val);
        }
    }


    /**
     * 测试设置扫描器缓存
     * <p>
     * JavaAPI 中, 不设置的话, 默认 cache 是 -1,DEFAULT_HBASE_CLIENT_SCANNER_CACHING = 2147483647;
     * 所以默认的是最大值, 但是貌似自己设置成10000会更快?
     * aching值越大(在内存可接受的情况下), 理论上速度越快
     *
     * @throws Exception
     */
    @Test
    public void getScanCache() throws Exception {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");
        Scan scan = new Scan();
//        System.out.println(scan.getCaching());
        scan.setCaching(5000);
        Table t = conn.getTable(tname);
        ResultScanner rs = t.getScanner(scan);
        long start = System.currentTimeMillis();
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            System.out.println(r.getColumnLatestCell(Bytes.toBytes("f1"), Bytes.toBytes("name")));
        }
        System.out.println(System.currentTimeMillis() - start);
    }


    /**
     * 测试缓存和批处理
     *
     * @throws Exception
     */
    @Test
    public void testBatchAndCaching() throws Exception {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");
        Scan scan = new Scan();
        scan.setCaching(-1);  // 设置缓存为-1, 默认就是最大缓存
        scan.setBatch(2);    // 一次推4列到客户端

        Table t = conn.getTable(tname);
        ResultScanner rs = t.getScanner(scan);
        Iterator<Result> it = rs.iterator();

        while (it.hasNext()) {
            Result r = it.next();
            // 每迭代一次, 打一个横线
            System.out.println("========================================");
            //得到一行的所有map,key=f1,value=Map<Col,Map<Timestamp,value>>
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = r.getMap();
            //
            for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : map.entrySet()) {
                //得到列族
                String f = Bytes.toString(entry.getKey());
                Map<byte[], NavigableMap<Long, byte[]>> colDataMap = entry.getValue();
                for (Map.Entry<byte[], NavigableMap<Long, byte[]>> ets : colDataMap.entrySet()) {
                    String c = Bytes.toString(ets.getKey());
                    Map<Long, byte[]> tsValueMap = ets.getValue();
                    for (Map.Entry<Long, byte[]> e : tsValueMap.entrySet()) {
                        Long ts = e.getKey();
                        String value = Bytes.toString(e.getValue());
                        System.out.print(f + "/" + c + "/" + ts + "=" + value + ",");
                    }
                }
            }
            System.out.println();
        }

        /**
         * 运行结果
         * ========================================
         f1/age/1529982061566=    ,f1/id/1529982061566=    ,
         ========================================
         f1/name/1529982061566=tom0,
         ========================================
         f1/age/1529982061730=   ,f1/id/1529982061730=   ,
         ========================================
         f1/name/1529982061730=tom1,
         */
    }


    /**
     * 测试 RowFilter 过滤器
     *
     * @throws IOException
     */
    @Test
    public void testRowFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");
        Scan scan = new Scan();

        // 行过滤器.  public RowFilter(比较器枚举, 比较器指定值)
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row000100")));
        scan.setFilter(rowFilter);

        Table t = conn.getTable(tname);
        ResultScanner rs = t.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            System.out.println(Bytes.toString(r.getRow()));
        }
    }


    /**
     * 列族过滤器
     * 测试FamilyFilter过滤器
     */
    @Test
    public void testFamilyFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");
        Scan scan = new Scan();

        FamilyFilter filter = new FamilyFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("f2")));
        scan.setFilter(filter);

        Table t = conn.getTable(tname);
        ResultScanner rs = t.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f2id = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            System.out.println(f1id + " : " + f2id);
        }
        /**
         * f2id 为空, 因为被过滤掉了
         */
    }


    /**
     * 测试QualifierFilter(列过滤器)
     */
    @Test
    public void testColFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");
        Scan scan = new Scan();

        QualifierFilter colfilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("id")));
        scan.setFilter(colfilter);

        Table t = conn.getTable(tname);
        ResultScanner rs = t.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f2id = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            byte[] f2name = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            System.out.println(f1id + " : " + f2id + " : " + f2name);
        }
    }


    /**
     * 测试ValueFilter(值过滤器) ValueFilter
     * 过滤value的值，含有指定的字符子串
     */
    @Test
    public void testValueFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");
        Scan scan = new Scan();

        ValueFilter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("tom110"));
        scan.setFilter(filter);

        Table t = conn.getTable(tname);
        ResultScanner rs = t.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.out.println(Bytes.toString(f1id) + " : " + Bytes.toString(f1name));
        }
    }


    /**
     * 参考列值过滤器 DependentColumnFilter
     * 过滤出指定列族, 列, value 的元素
     * 用的不多
     */
    @Test
    public void testDepFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");
        Scan scan = new Scan();
        DependentColumnFilter filter = new DependentColumnFilter(Bytes.toBytes("f1"),
                Bytes.toBytes("name"),
                false,
                CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes("tom918"))
        );

        scan.setFilter(filter);
        Table t = conn.getTable(tname);
        ResultScanner rs = t.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            byte[] name1 = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.out.println(Bytes.toString(name1));
        }
    }


    /**
     * 单列值value过滤，对列上的value进行过滤，不符合整行删除。
     * 此题过滤后的结果, 是把 列族为f1, 列为 name, value 为 tom997的 当前行过滤掉
     * 返回的就是符合过滤器规则的数据
     */
    @Test
    public void testSingleColumValueFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");
        Scan scan = new Scan();

        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("f1"),
                Bytes.toBytes("name"),
                CompareFilter.CompareOp.NOT_EQUAL,
                new BinaryComparator(Bytes.toBytes("tom997")));

        scan.setFilter(filter);
        Table t = conn.getTable(tname);
        ResultScanner rs = t.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.out.println(f1id + " : " + Bytes.toString(f1name));
        }
    }


    /**
     * 单列值排除过滤器,去掉过滤使用的列,对列的值进行过滤
     * 不包含过滤的内容
     * 经过测试: 是把 包含此过滤条件的列的所有的行 都排除掉
     * :TODO
     * 参考列不会被包含到结果中!!!
     */
    @Test
    public void testSingleColumValueExcludeFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");
        Scan scan = new Scan();

        SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter(Bytes.toBytes("f1"),
                Bytes.toBytes("id"),
                CompareFilter.CompareOp.EQUAL,
//                new BinaryComparaor(Bytes.toBytes("vv"))
                (byte[]) null
        );
        scan.setFilter(filter);
        Table t = conn.getTable(tname);
        ResultScanner rs = t.getScanner(scan);
        Iterator<Result> it = rs.iterator();

        while (it.hasNext()) {
            Result r = it.next();
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.out.println(f1id + " : " + Bytes.toString(f1name));
        }

    }


    /**
     * PrefixFilter
     * 前缀过滤,是rowkey过滤. where rowkey like 'row0002%'
     */
    @Test
    public void testPrefixFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");
        Scan scan = new Scan();
        PrefixFilter filter = new PrefixFilter(Bytes.toBytes("row0002"));

        scan.setFilter(filter);
        Table t = conn.getTable(tname);
        ResultScanner rs = t.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.out.println(f1id + " : " + Bytes.toString(f1name));
        }
    }


    /**
     * 分页过滤,是rowkey过滤,在region上扫描时，对每次page设置的大小。
     * 返回到到client，设计到每个Region结果的合并。
     * 参数为10, 一个区域取前10行
     */
    @Test
    public void testPageFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");
        Scan scan = new Scan();


        PageFilter filter = new PageFilter(10);

        scan.setFilter(filter);
        Table t = conn.getTable(tname);
        ResultScanner rs = t.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.out.println(f1id + " : " + Bytes.toString(f1name));
        }

        /**运行结果:
         * 此时可以再手动分区下, split '8658ac9ea01f80c2cca32693397a1e70','row000110',
         * 原来是2个区域, 20行,  现在是3个区域 30行
         [B@313ac989 : tom0
         [B@4562e04d : tom1
         [B@2a65fe7c : tom2
         [B@4135c3b : tom3
         [B@6302bbb1 : tom4
         [B@31304f14 : tom5
         [B@34a3d150 : tom6
         [B@2a4fb17b : tom7
         [B@5c6648b0 : tom8
         [B@6f1de4c7 : tom9
         [B@459e9125 : tom110
         [B@128d2484 : tom111
         [B@7cc0cdad : tom112
         [B@7c7b252e : tom113
         [B@4d5d943d : tom114
         [B@368f2016 : tom115
         [B@4c583ecf : tom116
         [B@692f203f : tom117
         [B@48f2bd5b : tom118
         [B@7b2bbc3 : tom119
         [B@a1153bc : tom551
         [B@1aafa419 : tom552
         [B@47eaca72 : tom553
         ....
         */
    }


    /**
     * keyOnly过滤器，只提取key,丢弃value.
     * A filter that will only return the key component of each KV (the value will
     * be rewritten as empty).
     * value 不显示, 只显示 key
     *
     * @throws IOException
     */
    @Test
    public void testKeyOnlyFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");
        Scan scan = new Scan();

        KeyOnlyFilter filter = new KeyOnlyFilter();

        scan.setFilter(filter);
        Table t = conn.getTable(tname);
        ResultScanner rs = t.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.out.println(f1id + " : " + Bytes.toString(f1name));
        }

        /**
         * 执行结果
         [B@2a65fe7c :
         [B@4135c3b :
         [B@6302bbb1 :
         [B@31304f14 :
         [B@34a3d150 :
         [B@2a4fb17b :
         [B@5c6648b0 :
         */
    }



    /**
     * ColumnPageFilter,列分页过滤器，过滤指定范围列，
     * select ,,a,b from ns1:t7
     *
     * 不管列族, 从左到右的列, 去掉前面2列, 从第3列开始, 取2列, 针对于所有的行
     * 表的结构是 age, id ,name
     * limit:2,  offset: 1
     * 因此就是从 id 开始2个, 因此 id,name 都有值
     */
    @Test
    public void testColumnPageFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");
        Scan scan = new Scan();

        ColumnPaginationFilter filter = new ColumnPaginationFilter(2,1);

        scan.setFilter(filter);
        Table t = conn.getTable(tname);
        ResultScanner rs = t.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            byte[] f1age = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age"));
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.out.println(f1age + " : "  + f1id + " : " + Bytes.toString(f1name));
        }
        /**
         null : [B@6302bbb1 : tom0
         null : [B@31304f14 : tom1
         null : [B@34a3d150 : tom2
         null : [B@2a4fb17b : tom3
         null : [B@5c6648b0 : tom4
         */
    }



    /**
     *  正则过滤
     */
    @Test
    public void testLike() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");
        Scan scan = new Scan();

        ValueFilter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator("^tom990")
        );

        scan.setFilter(filter);
        Table t = conn.getTable(tname);
        ResultScanner rs = t.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            byte[] f1age = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age"));
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.out.println(f1age + " : "  + f1id + " : " + Bytes.toString(f1name));
        }
    }


    /**
     * 复杂查询
     select * from t1 where ((age <= 13) and (name like '%t')
     or
     (age > 13) and (name like 't%'))

     注意: 这里的 13 在代码中不能是 string, 应该是 数值型

     * @throws IOException
     */
    @Test
    public void testComboFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");
        Scan scan = new Scan();

        //where ... f2:age <= 13
        SingleColumnValueFilter ftl = new SingleColumnValueFilter(
                Bytes.toBytes("f1"),
                Bytes.toBytes("age"),
                CompareFilter.CompareOp.LESS_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes(9))
        );

        //where ... f2:name like %t
        SingleColumnValueFilter ftr = new SingleColumnValueFilter(
                Bytes.toBytes("f1"),
                Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator("^tom9")
        );
        //ft
        FilterList ft = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        ft.addFilter(ftl);
        ft.addFilter(ftr);

        //where ... f2:age > 13
        SingleColumnValueFilter fbl = new SingleColumnValueFilter(
                Bytes.toBytes("f1"),
                Bytes.toBytes("age"),
                CompareFilter.CompareOp.GREATER,
                new BinaryComparator(Bytes.toBytes(13))
        );

        //where ... f2:name like %t
        SingleColumnValueFilter fbr = new SingleColumnValueFilter(
                Bytes.toBytes("f1"),
                Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator("t$")
        );
        //ft
        FilterList fb = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        fb.addFilter(fbl);
        fb.addFilter(fbr);


        // 这里 MUST_PASS_ONE 就是 or 的意思
        FilterList fall = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        fall.addFilter(ft);
        fall.addFilter(fb);

        scan.setFilter(fall);
        Table t = conn.getTable(tname);
        ResultScanner rs = t.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            byte[] f1age = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age"));
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.out.println(f1age + " : "  + f1id + " : " + Bytes.toString(f1name));
        }
    }





    /**
     *测试计数器
     */
    @Test
    public void testIncr() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t8");
        Table t = conn.getTable(tname);
        Increment incr = new Increment(Bytes.toBytes("row1"));
        incr.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("daily"),1);
        incr.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("weekly"),10);
        incr.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("monthly"),100);
        t.increment(incr);

    }



}





