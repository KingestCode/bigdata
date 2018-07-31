package com.rox.storm.grouping.shuffle;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
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

        // 通过实践发现, 这里设置NumTasks为2的话, 同样的数据会发2次
        builder.setSpout("spout-wordcount", new WordcountSpout()).setNumTasks(2);
        builder.setBolt("split-bolt", new SplitBolt(),1).shuffleGrouping("spout-wordcount").setNumTasks(1);

        // 这里用shuffleGrouping执行技术逻辑, 目前的理解是, 所有的 word 都是随机去的节点, 不一定所有相同key 的元素去同一个节点, 这样可以有效避免数据倾斜, 进行的 count 只是随机的 count, 不是最后的准确的结果
        builder.setBolt("count-bolt-1", new CountBolt(),1).shuffleGrouping("split-bolt").setNumTasks(1);

        // 这里才是进行最后的统计, 由于上一步已经进行了部分随机的 count 了, 所以这里只会最多为节点数量的请求, 不会产生倾斜
        builder.setBolt("count-bolt-2", new CountBolt(),3).fieldsGrouping("split-bolt",new Fields("word")).setNumTasks(3);  // 凡是相同的"word"都进同一个 group

        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.setDebug(true);

        /**
         * 本地模式
         */
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("2shuffle", conf, builder.createTopology());
//        Thread.sleep(10000);
//        cluster.shutdown();

//        StormSubmitter.submitTopology("wcdemo",conf,builder.createTopology());
    }

    /**
     * 执行结果暂存
     *
     * > 这里是在 localhost:8888 通过 nc -lk 8888, 监听搜集到的结果
     * x.local,45134,Thread-22-count-bolt-1-executor[3 3],CountBolt@89823333,(hello : 1)
     x.local,45134,Thread-22-count-bolt-1-executor[3 3],CountBolt@89823333,(hello : 3)
     x.local,45134,Thread-22-count-bolt-1-executor[3 3],CountBolt@89823333,(hello : 4)
     x.local,45134,Thread-22-count-bolt-1-executor[3 3],CountBolt@89823333,(hello : 6)
     x.local,45134,Thread-22-count-bolt-1-executor[3 3],CountBolt@89823333,(tomasLee : 2)
     x.local,45134,Thread-22-count-bolt-1-executor[3 3],CountBolt@89823333,(tomasLee : 3)
     x.local,45134,Thread-22-count-bolt-1-executor[3 3],CountBolt@89823333,(tomasLee : 4)
     x.local,45134,Thread-22-count-bolt-1-executor[3 3],CountBolt@89823333,(tom : 1)
     x.local,45134,Thread-22-count-bolt-1-executor[3 3],CountBolt@89823333,(tom : 2)

     x.local,45134,Thread-37-count-bolt-2-executor[4 4],CountBolt@1260553472,(tomasLee : 1)
     x.local,45134,Thread-37-count-bolt-2-executor[4 4],CountBolt@1260553472,(tomasLee : 2)
     x.local,45134,Thread-37-count-bolt-2-executor[4 4],CountBolt@1260553472,(tomasLee : 3)
     x.local,45134,Thread-37-count-bolt-2-executor[4 4],CountBolt@1260553472,(tomasLee : 4)
     x.local,45134,Thread-34-count-bolt-2-executor[5 5],CountBolt@1347648347,(tom : 2)
     x.local,45134,Thread-28-count-bolt-2-executor[6 6],CountBolt@242737241,(hello : 2)
     x.local,45134,Thread-28-count-bolt-2-executor[6 6],CountBolt@242737241,(hello : 3)
     x.local,45134,Thread-28-count-bolt-2-executor[6 6],CountBolt@242737241,(hello : 4)
     x.local,45134,Thread-28-count-bolt-2-executor[6 6],CountBolt@242737241,(hello : 5)
     x.local,45134,Thread-28-count-bolt-2-executor[6 6],CountBolt@242737241,(hello : 6)


     -------

     > 这里是控制台打印结果
     11826 [Thread-41] INFO  o.a.s.d.task - Emitting: count-bolt-1 default [tomasLee, 4]
     11827 [Thread-41] INFO  o.a.s.d.task - Emitting: count-bolt-1 default [tom, 2]
     11827 [Thread-41] INFO  o.a.s.d.task - Emitting: count-bolt-1 default [hello, 6]

     11841 [Thread-42] INFO  o.a.s.d.task - Emitting: count-bolt-2 default [hello, 6]
     11852 [Thread-43] INFO  o.a.s.d.task - Emitting: count-bolt-2 default [tom, 2]
     11860 [Thread-44] INFO  o.a.s.d.task - Emitting: count-bolt-2 default [tomasLee, 4]
     *
     */



}





