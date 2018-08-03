package com.rox.spark.scala


import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * 累加的 wordcount
  */
object KafkaStatefulWordCount{
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(3))

    // 如果要使用updateStateByKey累加历史数据, 那么要把每次的结果保存起来
    ssc.checkpoint("file:///Users/shixuanji/Documents/Code/Datas/updatebykey")

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
    val reduced: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)

    // 打印结果(Action)
    reduced.print()

    // 启动 sparkStreaming 程序
    ssc.start()

    // 等待优雅的退出
    ssc.awaitTermination()
  }


  /**
    * updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)]
    *
    */
  val updateFunc = (iter:Iterator[(String, Seq[Int],Option[Int])]) => {
    /**
      * 第一个参数：聚合的key，就是单词
      * 第二个参数：当前批次产生批次该单词在每一个分区出现的次数
      * 第三个参数：初始值或累加的中间结果
      */
//    iter.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))

    // 这里可以使用更高级的模式匹配
    iter.map{case(x, y, z) => (x, y.sum + z.getOrElse(0))}
  }
}









