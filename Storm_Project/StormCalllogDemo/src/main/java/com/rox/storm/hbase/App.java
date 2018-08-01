package com.rox.storm.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * 在 拼1 阶段之后, 直接把单词存入 hbase, 采用 hbase 的 increase 的机制, 实现单词统计&持久化
 */
public class App {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        //设置Spout
        builder.setSpout("spout", new WordCountSpout());
        //设置split-Bolt
        builder.setBolt("split-bolt", new SplitBolt()).shuffleGrouping("spout");
        //设置counter-Bolt
        builder.setBolt("hbase-bolt", new HBaseWCBolt()).shuffleGrouping("split-bolt");

        Config conf = new Config();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Storm-hbase", conf, builder.createTopology());
    }


    /**
     * 按照官网上的配置, 目前有问题
     * 直接在 bolt 中调用 hbase 的 javaAPI 解决
     */
    public static void hasbug(){
        //hbase映射
        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField("word")                //rowkey
                .withColumnFields(new Fields("word"))   //column
                .withCounterFields(new Fields("count")) //column
                .withColumnFamily("f1");                //列族

        Configuration hbaseConf = HBaseConfiguration.create();
        HBaseBolt hbaseBolt = new HBaseBolt("ns1:wordcount", mapper);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wcspout", new WordCountSpout()).setNumTasks(1);
        builder.setBolt("split-bolt", new SplitBolt(),2).shuffleGrouping("wcspout").setNumTasks(2);
        builder.setBolt("hbase-bolt", hbaseBolt,2).fieldsGrouping("split-bolt",new Fields("word")).setNumTasks(2);

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wc", conf, builder.createTopology());

        /**
         * 目前有 bug
         * java.lang.IllegalArgumentException: HBase configuration not found using key 'null'
         * 参考, 但是讲的不清楚
         * http://www.itkeyword.com/doc/7627980265225371333/hbase-configuration-not-found-using-key-null
         */
    }
}
