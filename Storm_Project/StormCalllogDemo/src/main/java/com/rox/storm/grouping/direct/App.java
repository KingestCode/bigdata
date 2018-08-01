package com.rox.storm.grouping.direct;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * allGrouping, 所有的 bolt 都会收到相同的拷贝 :TODO
 */
public class App {
    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        // 构建拓扑构造器
        TopologyBuilder builder = new TopologyBuilder();

        // 通过实践发现, 这里设置NumTasks为2的话, 同样的数据会发2次, 相当于2个水龙头
        builder.setSpout("spout-wordcount", new WordcountSpout()).setNumTasks(2);
        builder.setBolt("split-bolt", new SplitBolt(),2).allGrouping("spout-wordcount").setNumTasks(2);

        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.setDebug(true);

        /**
         * 本地模式
         */
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("allGrouping", conf, builder.createTopology());
//        Thread.sleep(10000);
//        cluster.shutdown();
//        StormSubmitter.submitTopology("wcdemo",conf,builder.createTopology());
    }

    /**
     * 这里会得到2份相同的数据,
     *
     * x.local,49517,Thread-19-split-bolt-executor[3 3],SplitBolt@2003035780,hello tomas
     x.local,49517,Thread-19-split-bolt-executor[3 3],SplitBolt@2003035780,hello tomson
     x.local,49517,Thread-19-split-bolt-executor[3 3],SplitBolt@2003035780,hello tomson
     x.local,49517,Thread-19-split-bolt-executor[3 3],SplitBolt@2003035780,hello tom
     x.local,49517,Thread-19-split-bolt-executor[3 3],SplitBolt@2003035780,hello tom

     ----

     x.local,49517,Thread-27-split-bolt-executor[4 4],SplitBolt@487915231,hello tomas
     x.local,49517,Thread-27-split-bolt-executor[4 4],SplitBolt@487915231,hello tomson
     x.local,49517,Thread-27-split-bolt-executor[4 4],SplitBolt@487915231,hello tomson
     x.local,49517,Thread-27-split-bolt-executor[4 4],SplitBolt@487915231,hello tom
     x.local,49517,Thread-27-split-bolt-executor[4 4],SplitBolt@487915231,hello tom


     *
     */


}





