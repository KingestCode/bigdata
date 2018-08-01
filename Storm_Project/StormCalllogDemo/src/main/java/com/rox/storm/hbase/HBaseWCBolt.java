package com.rox.storm.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;

public class HBaseWCBolt implements IRichBolt{

    Table t;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            Configuration conf = HBaseConfiguration.create();
            Connection conn = ConnectionFactory.createConnection(conf);
            TableName tname = TableName.valueOf("ns1:wordcount");
            t = conn.getTable(tname);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {

        String word = input.getString(0);
        Integer count = input.getInteger(1);        // 每次要添加的值

        //使用hbase的increment机制进行wordcount
        byte[] rowkey = Bytes.toBytes(word);            // 行
        byte[] f = Bytes.toBytes("f1");             // 列族
        byte[] c = Bytes.toBytes("count");          //列限定符

        try {
            t.incrementColumnValue(rowkey, f, c, count);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {

    }
}
