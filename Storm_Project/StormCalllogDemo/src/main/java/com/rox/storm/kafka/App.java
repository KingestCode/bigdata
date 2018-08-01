package com.rox.storm.kafka;

import com.rox.storm.grouping.shuffle.CountBolt;
import com.rox.storm.grouping.shuffle.WordcountSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.UUID;

/**
 * field 分组
     安装指定filed的key进行hash处理，
     相同的field，一定进入到同一bolt.
     该分组容易产生数据倾斜问题，通过使用二次聚合避免此类问题。

    这里遇到一个 bug

    本地的 kafka 重复消费的问题,
    由于自己写的blot继承于IBolt, 但自己没有在代码中显示的调用collector.ack();
    导致kafkaSpout一直认为emitted的数据有问题, 超时之后进行数据重发
    如果Bolt不进行ack, 则红色代码处的offsetNumber永远相等, 导致一直不进行offset的回写操作
    见这里
 https://www.cnblogs.com/zhwbqd/p/3977417.html

 */
public class App {
    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        String zkConnString = "cs1:2181,cs2:2181";
        // 通过 拿到 zk 主机对象
        BrokerHosts hosts = new ZkHosts(zkConnString);
        String topicName = "kafka-test";

        // 配置 SpoutConfig  (org.apache.storm.kafka 包下的)
        // console 中获取主题: kafka-topics.sh --zookeeper cs2:2181 --list
        // 其实就是拿到主题的信息
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        // 根据 spoutConfig 创建 KafkaSpout
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        // 构建 Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", kafkaSpout).setNumTasks(2);
        builder.setBolt("kafka-bolt", new SplitBolt(),2).shuffleGrouping("kafka-spout");

        // 配置 storm 的 config
        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.setDebug(true);

        // 本地模式提交
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("storm-kafka", conf, builder.createTopology());
    }

}





