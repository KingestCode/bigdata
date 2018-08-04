package com.rox.spark.scala.suanzi

import org.apache.spark.{SparkConf, SparkContext}

/**
  * When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs.
  * 按照数据集中某一个 key 分组
  */
object GroupByKeyDemo1_分组 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    conf.setAppName("WordCountScala")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("/Users/shixuanji/Documents/IDEs/iTerm2/scala/stus.txt")

    val rdd2 = rdd1.map(line=>{
      val key = line.split(" ")(3)
      // 返回一个(k,v)元祖
      (key,line)
    })

    // 返回值类型 RDD[(K, Iterable[V])
    val rdd3 = rdd2.groupByKey()

    // collect() 是得到集合, 集合中的每个元素是元祖
    rdd3.collect().foreach(t =>{
      // key: t._1, value: t._2
      val key = t._1
      println(key + "================")

      for (e <- t._2) {
        println(e)
      }
    })


  }

}
