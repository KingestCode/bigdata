package com.rox.spark.scala

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}


/**
  * 初版
  * @see  KafkaDirectWordCount
  *
  * 直连: 本质上就是创建一个 Kafka 的 inputDStream 的迭代器, kafka 产生一个数据, 这边就按照批次消费
  *
  */
object KafkaDirectWordCount2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val brokerList = "cs2:9092,cs3:9092,cs4:9092"
    val zkQuorum = "cs1:2181,cs2:2181,cs3:2181,"
    val topic = "wordcount"
    val topics = Set(topic)
    val group = "g001"


    val kafkaParams = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> group,
      //从头开始读取数据
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )
    val zkClient = new ZkClient(zkQuorum)

    val topicDirs = new ZKGroupTopicDirs(group, topic)
    val zkTopicPath: String = s"${topicDirs.consumerOffsetDir}"   // ../topic => 拿到 zk 中存储的 consumerffset 的路径
    val childrenNum: Integer = zkClient.countChildren(zkTopicPath)    // 拿到路径 ../topic 下的子节点(分区)

    var kafkaStream: InputDStream[(String, String)] = null     // 定义kafkaStream 流对象
    var fromOffsets: Map[TopicAndPartition, Long] = Map()      // 以topic+partition 为 key, offset 为 value, 定义集合 Map 保证唯一性

    if (childrenNum > 0) {        // 存在分区信息
      for (i <- 0 until childrenNum) {
        val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")   // 取出分区对应信息--> offset
        val tp = TopicAndPartition(topic, i)    // 创建 tp 对象 (主题,分区)
        fromOffsets += (tp -> partitionOffset.toLong)
      }
      println("-------打印取出来的主题,分区 和 offset-------")
      fromOffsets.foreach(println)
      println("------------------------------------------")

      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())   //定义 kafka 消息的 处理函数

      kafkaStream = KafkaUtils.createDirectStream
        [String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)   // 拿到 kafkaDirectStream
    }
    else {    // zk 中不存在分区信息, 就直接(从头)读取主题
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }

    /**
      * 遍历kafkaStream
      * 数据收集到 kafka 中的时候, 对应的OffsetRange 啥的就已经确定了, 比如当前的 untilOffset 是多少
      * 这里只是拿到了这个流, 从中读取数据后, 顺便再把这个offset 存到zk 中去罢了
      */
    kafkaStream.foreachRDD { rdd =>                                                             // 迭代

      /**
        * OffsetRange(topic: 'wordcount', partition: 0, range: [20 -> 20])
          OffsetRange(topic: 'wordcount', partition: 1, range: [19 -> 19])
          OffsetRange(topic: 'wordcount', partition: 2, range: [19 -> 19])
        */
      var offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges     // 强转为HasOffsetRanges, 取出 kafka 中的 offsetRanges(是一个由 OffsetRange 组成的数组)

      // 这里打印的都是从 kafka 读到的实时流的 信息
      println("-------遍历offsetRanges数组----------------")
      offsetRanges.foreach(x => println(x))
      println("------------------------------------------")

      val inputV  = rdd.map(_._2)                                                               // 从 kafkaStream 中取出 真正的内容

      inputV.foreachPartition(partition =>                                                      //对 RDD 进行操作, 触发 Action
        //++++++++++++++++++++++++++++++++++++//++++++++++++++++++++++++++++++++++++
        partition.foreach(println)        // 这里处理所有的业务逻辑代码, 这里仅仅只是做了打印
        //++++++++++++++++++++++++++++++++++++//++++++++++++++++++++++++++++++++++++
      )

      // 这里其实就相当于: 读取一个 (DStream) 的数据, 就往 zk 中更新一次 offset
      for (o <- offsetRanges) {                                                                  // 从存储偏移量数组中取出值
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"                            // 从 zk 中取出当前消费的主题分区路径模板, 拼接上当前读到的分区, 更新上最新读到哪里
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
      }
    }
    // 启动
    ssc.start()

    // 优雅的等待结束
    ssc.awaitTermination()
  }
}


