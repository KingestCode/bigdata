package com.rox.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求rdd并集，但是不去重
  * Union 就是把两者合并起来
  * Returns a new RDD that contains the intersection of elements in the source dataset and the argument dataset and the argument.
  */
object UnionDemo1 {
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

    val allRDD = infoRDD.union(warnRDD)
    println("-----------allRDD")
    allRDD.collect().foreach(println)

  }

}
