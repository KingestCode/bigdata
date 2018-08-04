package com.rox.spark.java.SparkStreaming_java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Some;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 *  可用于跨批次统计 updateStateByKey
 *  可以记录旧值
 */
public class WCSparkStreamWindowApp {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName("wc");
        conf.setMaster("local[4]");
        //创建Spark流应用上下文
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Seconds.apply(2));

        // 创建检查点
        jsc.checkpoint("file:///Users/shixuanji/Documents/Code/temp/test");

        //创建socket离散流  ----注意: 端口问题
        JavaReceiverInputDStream sock = jsc.socketTextStream("localhost",10086);

        //压扁
        JavaDStream<String> wordsDS = sock.flatMap(new FlatMapFunction<String,String>() {
            public Iterator call(String str) {
                List<String> list = new ArrayList<String>() ;
                String[] arr = str.split(" ");
                for(String s : arr){
                    list.add(s);
                }
                return list.iterator();
            }
        });

        //映射成元组
        JavaPairDStream<String,Integer> pairDS = wordsDS.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String,Integer>(s,1);
            }
        }) ;


        //聚合
        /** 统计同一 window 下的 key 的聚合 (用的比较多...)
         * def reduceByKeyAndWindow(reduceFunc: Function2[V, V, V],
             windowDuration: Duration,
             slideDuration: Duration): JavaPairDStream[K, V]
         >>> Return a new DStream by applying `reduceByKey` over a sliding window
         */
        JavaPairDStream<String,Integer> countDS = pairDS.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) {
                return v1 + v2;
            }
        },Seconds.apply(6),Seconds.apply(4));


        /**
         * 统计 window 下的 key 的个数
         *
         */
        JavaDStream<Long> longJavaDStream = pairDS.countByWindow(Seconds.apply(6), Seconds.apply(4));


        //打印
        countDS.print();
        System.out.println("~~~~~~~~~~~~~~~~");
        longJavaDStream.print();

        jsc.start();

        jsc.awaitTermination();

//        jsc.stop();
    }
}