package com.rox.spark.java.sql_hive_spark;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.hive.HiveSessionState;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Properties;

/**
 * 如果不加上  .enableHiveSupport(), 创建的就是本地数据 ??
 */
public class SQLHiveJava_整合hive {
    public static void main(String[] args) {
//        SparkConf conf = new SparkConf();
//        conf.setMaster("local") ;
//        conf.setAppName("SQLJava_sql语法");
        System.setProperty("HADOOP_USER_NAME","ap");

        SparkSession sess = SparkSession.builder()
                .appName("HiveSQLJava")
                .config("spark.master","local")
                .enableHiveSupport()
                .getOrCreate();

//        Dataset<Row> df = sess.sql("create table mytt(id int)");

        sess.sql("use mydb_test");
//        Dataset<Row> df = sess.sql("show tables");
        Dataset<Row> df = sess.sql("select * from new_help_keyword limit 10");
        df.show();
    }
}