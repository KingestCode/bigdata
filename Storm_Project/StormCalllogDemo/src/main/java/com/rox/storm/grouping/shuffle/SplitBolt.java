package com.rox.storm.grouping.shuffle;

import com.rox.storm.util.Util;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitBolt implements IRichBolt {

    private TopologyContext context;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.context = context;
        this.collector = collector;
    }

    /**
     * 拿到 spout 传过来的数据做的处理
     * @param input
     */
    @Override
    public void execute(Tuple input) {
        // 注意, 此处的元祖是从0开始
        String line = input.getString(0);
        String[] arr = line.split(" ");
        for(String s : arr){
            collector.emit(new Values(s, 1));
            // print the first time split words
//            Util.sendToLocalHost(this,"first time split word: (" + s + ",1)");
        }
    }

    @Override
    public void cleanup() {

    }

    /**
     * 输出字段声明
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
