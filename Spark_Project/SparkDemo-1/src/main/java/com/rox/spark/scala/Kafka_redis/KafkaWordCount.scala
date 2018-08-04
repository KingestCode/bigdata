package com.rox.spark.scala.Kafka_redis

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * SparkStreaming-Kafka
  */
object KafkaWordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(3))

    val zkQuorum = "cs1:2181,cs2:2181,cs3:2181"
    val groupId = "g1"
    val topic = Map[String, Int]("kafka-test" -> 1)

    // 创建 DStream, 需要 KafkaDStream
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topic)

    //对数据进行处理
    //Kafak的ReceiverInputDStream[(String, String)]里面装的是一个元组（key是写入的key，value是实际写入的内容）
    val lines: DStream[String] = data.map(_._2)

    println("这里打印的是 元祖 中取出的第一个元素--key" + data.map(_._1).toString)

    //对DSteam进行操作，你操作这个抽象（代理，描述），就像操作一个本地的集合一样
    //切分压扁
    val words: DStream[String] = lines.flatMap(_.split(" "))

    // 单词和1 组合
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))

    // 聚合
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)

    // 打印结果
    reduced.print()

    // 启动 sparkStreaming 程序
    ssc.start()

    // 等待优雅的退出
    ssc.awaitTermination()
  }
}
