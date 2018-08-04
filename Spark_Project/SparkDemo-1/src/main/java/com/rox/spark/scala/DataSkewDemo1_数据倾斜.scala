package com.rox.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object DataSkewDemo1_数据倾斜 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("DataSkewDemo1")
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("/Users/shixuanji/Documents/IDEs/iTerm2/scala/a.txt",4)

    /**
      *  解决数据倾斜:
      *  1> 先对每个 word 添加 个数 (word,1)
      *  2> 取出 word, 在 word后随机拼接 _num, 然后再与 v 组成 (newK, v)元祖
      *  3> 对新的 (k,v)进行 reduceByKey, 统计出每个 k 的个数 (newK, count)
      *  4> 对结果继续进行 map, 去掉 _num, 把原本的 k 取出来, 再跟 count 组成新的元祖 (k,count)
      *  5> 继续进行 reduceByKey, 此时并发量最多也只是随机数的个数了, 不会产生严重的数据倾斜, 计算出最终的结果
      *  6> 存到文件中 saveAsTextFile
      */
    rdd1.flatMap(_.split(" ")).map((_,1)).map(t => {
      val w1 = t._1
      val n = Random.nextInt(100)
      (w1 + "_" + n, t._2)
    }).reduceByKey(_ + _,4).map(t => {
      val w2 = t._1
      val count = t._2
      val w3 = w2.split("_")(0)
      (w3, count)
    }).reduceByKey(_ + _,4).saveAsTextFile("/Users/shixuanji/Documents/IDEs/iTerm2/scala/DataSkew")


    //reduceByKey(..) : 对相同的 key 的 v 做处理
  }

}
