package com.rox.storm.wordcount;

import com.rox.storm.util.Util;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class WordcountSpout implements IRichSpout {

    private TopologyContext context;
    private SpoutOutputCollector collector;

    private List<String> list;

    private Random r = new Random();

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        Util.sendToClient(this,"open()",7777);

        this.context = topologyContext;
        this.collector = spoutOutputCollector;

        list = new ArrayList<>();
        list.add("hello tom");
        list.add("hello tomas");
        list.add("hello tomasLee");
        list.add("hello tomson");
    }

    @Override
    public void close() {
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    /**
     * 处理数据为元祖, 发射出去
     */
    @Override
    public void nextTuple() {
        Util.sendToClient(this,"nextTuple()",7777);

        // 取出从0到3
        String line = list.get(r.nextInt(4));
        this.collector.emit(new Values(line));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    /**
     * 定义输出字段
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
