package com.rox.storm.grouping.ensure;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * bolt 端在 execute 方法中确认消息时, 根绝成功或失败调用 ack(tuple), fail(tuple)
 * 此时会回调到 发送端 的 ack() 和 fail
 */
public class SplitBolt implements IRichBolt {

    private TopologyContext context ;
    private OutputCollector collector ;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.context = context ;
        this.collector = collector ;
    }

    public void execute(Tuple tuple) {
        String line = tuple.getString(0);
        if(new Random().nextBoolean()){
            //确认
            collector.ack(tuple);
            System.out.println(this + " : ack() : " + line + " : "+ tuple.getMessageId().toString());
        }
        else{
            //失败
            collector.fail(tuple);
            System.out.println(this + " : fail() : " + line + " : " + tuple.getMessageId().toString());
        }
    }

    public void cleanup() {

    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"));

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
