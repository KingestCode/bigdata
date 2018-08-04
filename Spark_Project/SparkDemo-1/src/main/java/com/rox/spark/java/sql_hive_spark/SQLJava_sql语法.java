package com.rox.spark.java.sql_hive_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.function.Consumer;

public class SQLJava_sql语法 {

    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .appName("SQLJava_sql语法")
                .config("spark.master","local")
                .getOrCreate();

        // 读取 json 到 DataFrame
        Dataset<Row> df1 = session.read().json("/Users/shixuanji/Documents/IDEs/iTerm2/Json/test.json");

        //  创建临时视图
        df1.createOrReplaceTempView("customers");
        df1.show();
        System.out.println("---------------");

        // 按照 api 查询
//        Dataset<Row> df3 = df1.where("age > 13");
//        df3.show();

        // 按照 sql 方式查询
        Dataset<Row> df2 = session.sql("select * from customers where age > 13");
        df2.show();
        System.out.println("---------------");

        // 聚合查询
        Dataset<Row> dfCount = session.sql("select count(1) from customers");
        dfCount.show();


        // DataFrame 和 RDD 互操作
        JavaRDD<Row> rdd = df1.toJavaRDD();

        rdd.collect().forEach(new Consumer<Row>() {
            @Override
            public void accept(Row row) {
                long age = row.getLong(0);
                long id = row.getLong(1);
                String name = row.getString(2);
                System.out.println(age + "," + id + "," + name);
            }
        });

        // 保存为 json , 并不是一个 json 文件, 每行是一个 json 对象
        df1.write().mode(SaveMode.Append).json("file:///Users/shixuanji/Documents/IDEs/iTerm2/Json/2018-07-17-1");
    }
}
