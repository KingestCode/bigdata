package com.rox.spark.scala.suanzi

import org.apache.spark.{SparkConf, SparkContext}

/**
  * intersection 求交集,提取两个rdd中都含有的元素。
  * Returns a new RDD that contains the intersection of elements in the source dataset and the argument.
  */
object IntersectionDemo1_交集 {

    def main(args: Array[String]): Unit = {

      val conf = new SparkConf()
      conf.setAppName("UnionDemo1")
      conf.setMaster("local[4]")
      val sc = new SparkContext(conf)

      val rdd1 = sc.textFile("/Users/shixuanji/Documents/zookeeper.out", 4)

      // 所有的 INFO
      val infoRDD = rdd1.filter(_.toLowerCase.contains("info"))
      println("-----------infoRDD")
      infoRDD.collect().foreach(println)
      // 所有的 WARN
      val warnRDD = rdd1.filter(_.toLowerCase.contains("warn"))
      println("-----------warnRDD")
      warnRDD.collect().foreach(println)

      // 交集
      val intersectionRDD = infoRDD.intersection(warnRDD)
      println("-----------intersectionRDD")
      intersectionRDD.collect().foreach(println)

    }

}
