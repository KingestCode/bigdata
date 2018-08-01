package com.rox.storm.grouping.custom;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * 自定义分组
 * 就是手动设置接收目标任务的 task 的接受规则
 * 此时自定义的只会分给四个 task 中的 0,1,2, 有一个 task 对象没有分到
 */
public class App {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        //设置Spout
        builder.setSpout("wcspout", new WordCountSpout()).setNumTasks(2);
        //设置creator-Bolt
        builder.setBolt("split-bolt", new SplitBolt(),4).customGrouping("wcspout",new MyGrouping()).setNumTasks(4);

        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.setDebug(true);

        /**
         * 本地模式storm
         */
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wc", conf, builder.createTopology());
        System.out.println("hello world");
    }
}
