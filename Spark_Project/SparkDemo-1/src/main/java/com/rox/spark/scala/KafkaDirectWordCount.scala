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


object KafkaDirectWordCount {
  def main(args: Array[String]): Unit = {

      //创建SparkConf
      val conf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[2]")

      //创建SparkStreaming，并设置间隔时间
      val ssc = new StreamingContext(conf, Seconds(5))

      //执行 kafka 的 broker 地址( SparkStreaming 的 Task 直连到 Kafka 的分区上, 用更加底层的API消费，效率更高)
      val brokerList = "cs2:9092,cs3:9092,cs4:9092"

      //指定 ZK 的地址, 后期更新消费的偏移量时使用(以后可以使用 Redis. MySQL来记录偏移量)
      val zkQuorum = "cs1:2181,cs2:2181,cs3:2181,"

      //指定消费的 topic 名字
      val topic = "wordcount"

      // 创建 Stream 时使用的 topic 名字集合, SparkStreaming 可以同时消费多个 topic, 后面用到
      val topics: Set[String] = Set(topic)

      //指定组名
      val group = "g001"

      //创建一个 ZKGroupTopicDirs 对象,其实是指定往zk中写入数据的目录，用于保存偏移量
      val topicDirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(group, topic)

      //获取 Zookeeper 中的 topic 路径 "/g001/offsets/wordcount/"
      val zkTopicPath: String = s"${topicDirs.consumerOffsetDir}"
      println("zk中 topic 的路径----" + zkTopicPath)

      //准备 Kafka 的参数
      val kafkaParams = Map(
        "metadata.broker.list" -> brokerList,
        "group.id" -> group,
        //从头开始读取数据
        "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
      )

      //传入 zookeeper 的host 和 ip，创建一个zk 的 client,用于从 zk 中读取 & 更新偏移量数据
      val zkClient = new ZkClient(zkQuorum)

      // 查询该路径下是否有子节点 (子节点是保存不同 partition 时生成的）
      // /g001/offsets/wordcount/0/10001"
      // /g001/offsets/wordcount/1/30001"
      // /g001/offsets/wordcount/2/10001"
      //zkTopicPath  -> /g001/offsets/wordcount/
      val children = zkClient.countChildren(zkTopicPath)

      var kafkaStream: InputDStream[(String, String)] = null

      // 如果 zk 中有保存 offset, 我们会利用这个 offset 作为 kafka 的起始位置
      var fromOffsets: Map[TopicAndPartition, Long] = Map()

      // 如果保存过 offset
      if (children > 0) {
        for (i <- 0 until children) {

          // 读取 /g001/offsets/wordcount/0 分区的数据
          val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")
          // /g001/offsets/wordcount/0 -> 10001
          val tp = TopicAndPartition(topic, i)

          //将不同 partition 对应的 offset 增加到 fromOffsets的 Map 集合中( 就是往 fromOffsets 中添加集合数据)
          // ((wordcount,0) -> 10001)  ====>  ((主题,分区) -> 偏移量)
          // var 的 不可变 Map, 添加(k,v)对时, 用 += / + 都可以
          fromOffsets += (tp -> partitionOffset.toLong)
        }
        //Key: kafka的key   values: "hello tom hello jerry"
        //这个函数会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (kafka的key, message) 这样的 tuple
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())

        //通过KafkaUtils创建直连的DStream（fromOffsets参数的作用是:按照前面计算好了的偏移量继续消费数据）
        //[String, String, StringDecoder, StringDecoder,     (String, String)]
        //  key    value    key的解码方式   value的解码方式
        kafkaStream = KafkaUtils.
          createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
      }
      else {
        // 如果 zk 中没有保存已经读取的 offset, 就直接读取 topics
        kafkaStream = KafkaUtils.
          createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      }

      // 在外部定义偏移量范围数组
      var offsetRanges = Array[OffsetRange]()

      /**
        * 从kafka读取的消息，DStream的Transform方法可以将当前批次的RDD获取出来
        * 该transform方法计算获取到当前批次RDD,然后将RDD的偏移量取出来，然后在将RDD返回到DStream
        * 事实上这里的返回值, 依然是 kafkaStream, 只是换了一个名字  transform { rdd => rdd }
        */
      val transform: DStream[(String, String)] = kafkaStream.transform { rdd =>
        //得到该 rdd 对应 kafka 的消息的 offset
        //将KafkaRDD强转为 HasOffsetRanges，从中可以取到偏移量的范围
        /**
          * 之前就是这里出了 bug
          */
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }

      //从kafkaStream[String, String] 元祖中 中取出 value, 也就是真正的要处理的数据
      val messages: DStream[String] = transform.map(_._2)

      //迭代 DStream 中的 RDD
      messages.foreachRDD { rdd =>

        //对 RDD 进行操作, 触发 Action
        rdd.foreachPartition(partition =>

          // 这里处理所有的业务逻辑代码, 这里仅仅只是做了打印
          partition.foreach(println)
        )

        // 从存储偏移量数组中取出值
        for (o <- offsetRanges) {
          // 从 zk 中取出当前消费的主题分区
          val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
          // 将该 partition 的 offset 保存到 zookeeper, 类似于 /g001/offsets/wordcount/0/20000
          ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
        }
      }
      // 启动
      ssc.start()

      // 等待结束
      ssc.awaitTermination()
    }
}



