package com.rox.spark.java;

import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.*;
//import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

public class KafkaSparkstreamingDemo {
    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf();
        conf.setAppName("KafkaSparkstreamingDemo");
        conf.setMaster("local[4]");
        //创建Spark流应用上下文
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Seconds.apply(2));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "cs2:9092,cs3:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "g6");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("kafka-test");

        /*// 取出 kafka stream
        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        // 压扁
        JavaDStream<String> wordDS = stream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> r) throws Exception {
                String value = r.value();
                List<String> list = new ArrayList<>();
                String[] arr = value.split(" ");
                for (String s : arr) {
                    list.add(s);
                }
                return list.iterator();
            }
        });

        // 映射成元祖 (拼1)
        JavaPairDStream<String,Integer> pairDS = wordDS.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });
        
        
        // 聚合
        JavaPairDStream<String, Integer> countDS = pairDS.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 打印计算结果
        countDS.print();

        streamingContext.start();

        streamingContext.awaitTermination();*/
    }
}
