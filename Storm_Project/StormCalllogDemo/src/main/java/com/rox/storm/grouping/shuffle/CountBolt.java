package com.rox.storm.grouping.shuffle;

import com.rox.storm.util.Util;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CountBolt implements IRichBolt {

    private Map<String, Integer> wcMap;

    private TopologyContext context;
    private OutputCollector collector;

    private long duration = 5000;   // a time slice

    /**
     * 放一些初始化的东西
     * @param stormConf
     * @param context
     * @param collector
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.context = context;
        this.collector = collector;
        wcMap = new HashMap<>();

        /**
         * Collections 工具类 将 map 转为同步的 map
         */
        wcMap = Collections.synchronizedMap(wcMap);

        /**
         * 开启分线程, 循环执行清理分发 map 中数据的工作, 注意线程安全问题
         */
        Thread t = new Thread(){
            public void run(){
                while (true){
                    // 发送数据
                    emitData();
                }
            }
        };
        ///// 设置为守护线程 ///////
        t.setDaemon(true);
        t.start();
    }

    private void emitData() {
        // 同步锁 发送+清理 过程
        synchronized (wcMap){
            for (Map.Entry<String, Integer> entry : wcMap.entrySet()) {
                //向下一环节发送数据
                collector.emit(new Values(entry.getKey(), entry.getValue()));
            }
            // 清空 map
            wcMap.clear();
        }

        /**
         * 提交一次后 休眠5秒, 下次提交前5秒时间片内的数据
         */
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        // 提取单词
        String word = input.getString(0);
        // 提取数量
        Integer count = input.getInteger(1);

        if (!wcMap.containsKey(word)) {
            wcMap.put(word, 1);
        }else {
            wcMap.put(word, wcMap.get(word) + count);
        }

        // 发送此时的 map
        Util.sendToLocalHost(this, "("+ word + " : " + wcMap.get(word).toString() + ")");
    }

    @Override
    public void cleanup() {
//        wcMap.entrySet().forEach(System.out::println);

        for (Map.Entry<String,Integer> entry : wcMap.entrySet()){
            System.out.println(entry.getKey() + " ======== " + entry.getValue());
            System.out.println("\n\n\n\n\n\n\n\n");
        }
    }

    /**
     * 注意: 这里会先进行一次 shuffleGrouping, 所以还得声明输出字段
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
