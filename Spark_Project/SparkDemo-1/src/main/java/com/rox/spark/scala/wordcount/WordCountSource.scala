package com.rox.spark.scala.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCountSource {

  def main(args: Array[String]): Unit = {
    //创建Spark配置对象
    val conf = new SparkConf()
    conf.setAppName("WordCountSource")

    //设置master属性
    conf.setMaster("local") ;

    //通过conf创建sc
    val sc = new SparkContext(conf)

    //加载文本文件
    val rdd1 = sc.textFile("file:///Users/shixuanji/Documents/IDEs/iTerm2/scala/a.txt");

    //压扁
    val rdd2 = rdd1.flatMap(_.split(" ")) ;

    //映射w => (w,1)
    val rdd3 = rdd2.map((_,1))

    //按照key聚合，不指定分区数
    val rdd4 = rdd3.reduceByKey(_ + _)

    val rdd5 = rdd4.collect()

    val rdd6 = rdd5.foreach(println)
  }
}
