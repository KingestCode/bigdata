package com.rox.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WordCountJava {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
//        conf.setAppName("wordCountJava");
//        conf.setMaster("local");

        // 创建 sc 对象
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // 加载文件
        JavaRDD<String> rdd1 = sc.textFile(args[0]);
        
        // 压扁
        JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String >() {
            public Iterator<String> call(String s) {
                List<String> list = new ArrayList<>();
                String[] arr = s.split(" ");
                for (String ss : arr) {
                    list.add(ss);
                }
                return list.iterator();
            }
        });

        // 映射 word => (word,1)
        JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });


        // 聚合 reduceByKey (对2个参数进行聚合产生一个新的值)
        JavaPairRDD<String, Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) {
                return v1 + v2;
            }
        });


        // 收集,打印输出
        for (Object o : rdd4.collect()) {
            System.out.println(rdd4.collect());
        }
    }
}
