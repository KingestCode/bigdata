package com.rox.storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class App {
    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        // 构建拓扑构造器
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout-wordcount", new WordcountSpout(),3).setNumTasks(4);
        builder.setBolt("split-bolt", new SplitBolt(),4).shuffleGrouping("spout-wordcount");  //从哪里拿数据 shuffle
        builder.setBolt("count-bolt", new CountBolt(),5).fieldsGrouping("split-bolt",new Fields("word"));  // 凡是相同的"word"都进同一个 group

        Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setDebug(true);

        /**
         * 本地模式
         */
        /*LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wcdemo", conf, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();*/

        StormSubmitter.submitTopology("wcdemo",conf,builder.createTopology());
    }
}
