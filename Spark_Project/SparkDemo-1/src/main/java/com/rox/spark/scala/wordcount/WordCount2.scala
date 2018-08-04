package com.rox.spark.scala.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("MyWordCount_Scala")

    // local 后面设置的数量是 本地的模拟线程数
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 路径后面的数字是自己设置的分区数量
    val rdd1 = sc.textFile("/Users/shixuanji/Documents/a.txt",2);

    val rdd2 = rdd1.flatMap(line => {line.split(" ")}).filter(_.contains("wor"))


    /**
      * mapPartitions的使用
      */
    // 在每个分区前打印一句话
/*
    val rdd3 = rdd2.mapPartitions(it => {
      import scala.collection.mutable.ArrayBuffer
      val buf = ArrayBuffer[String]()
      // 拿到当前线程, 并打印出来
      val tname = Thread.currentThread().getName
      println(tname + " : mapPartitions start")
      println("----------------------------\n")
      // 遍历每个已经 flat的 单词, 在前面添加上 _
      for (e <- it) {
        buf.+=("_"+e)
      }
      // 返回处理过的迭代器
      buf.iterator
    })
*/


    /**
      *  mapPartitionsWithIndex
      */

    // 在每个分区前打印一句话, 并且打印当前分区数
    val rdd3 = rdd2.mapPartitionsWithIndex((index,it) => {
      import scala.collection.mutable.ArrayBuffer
      val buf = ArrayBuffer[String]()
      // 拿到当前线程, 并打印出来
      val tname = Thread.currentThread().getName
      println("线程名--"+tname + "\n当前分区--"+ index  +  " :: mapPartitions start")
      println("----------------------------\n")
      // 遍历每个已经 flat的 单词, 在前面添加上 _
      for (e <- it) {
        buf.+=("_"+e)
      }
      // 返回处理过的迭代器
      buf.iterator
    })


    // 每个word 后面都拼接上1, 并且打印出处理每个 word 的线程
    val rdd5 = rdd3.map(word => {
      val tname = Thread.currentThread().getName
      println(tname + ": map(word => (word,1)) 的 word : " + word)
      (word, 1)})

    val rdd4 = rdd5.reduceByKey(_ + _)
    val r = rdd4.collect()

    r.foreach(println)
  }
}
