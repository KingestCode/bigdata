package com.rox.storm.wordcount;

import com.rox.storm.util.Util;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class CountBolt implements IRichBolt {

    private Map<String, Integer> wcMap;

    private TopologyContext context;
    private OutputCollector collector;

    /**
     * 放一些初始化的东西
     * @param stormConf
     * @param context
     * @param collector
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        Util.sendToClient(this,"prepare()",9999);
        this.context = context;
        this.collector = collector;

        wcMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        Util.sendToClient(this,"execute()",9999);
        String word = input.getString(0);
        Integer count = input.getInteger(1);

        if (!wcMap.containsKey(word)) {
            wcMap.put(word, 1);
        }else {
            wcMap.put(word, wcMap.get(word) + count);
        }
    }

    @Override
    public void cleanup() {
//        wcMap.entrySet().forEach(System.out::println);

        for (Map.Entry<String,Integer> entry : wcMap.entrySet()){
            System.out.println(entry.getKey() + " ======== " + entry.getValue());
            System.out.println("\n\n\n\n\n\n\n\n");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
