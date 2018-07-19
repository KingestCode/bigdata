package com.rox.spark.scala

import org.apache.spark._
import org.apache.spark.streaming._


object SparkStreamingDemo {

  def main(args: Array[String]): Unit = {

    // local[n]必须 > 1, 因为需要分线程不断去取
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingDemo")

    // 创建 Spark 流上下文, 批次时长是 5s(隔5秒读取一个批次)
    val ssc = new StreamingContext(conf,Seconds(2))

    // 创建socket 文本流,Creates an input stream from TCP source hostname:port
    val lines = ssc.socketTextStream("localhost",9999)

    // 分割,压扁
    val words = lines.flatMap(_.split(" "))

    // 变换成对偶
    val pairs =  words.map((_,1))

    // 化简, 聚合
    val count = pairs.reduceByKey(_ + _)

    count.print()   // 打印

    // 启动流
    ssc.start()

    // 等待结束
    ssc.awaitTermination()

//    ssc.stop(false)
  }

}
