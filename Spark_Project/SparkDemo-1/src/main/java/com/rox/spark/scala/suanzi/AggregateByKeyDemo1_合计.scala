package com.rox.spark.scala.suanzi

import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyDemo1_合计 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("AggregateByKeyDemo1")
    conf.setMaster("local[4]");
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("file:///Users/shixuanji/Documents/IDEs/iTerm2/scala/a.txt", 4)
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.map((_, 1))

    def seq(a : Int, b : Int) : Int = {
      Math.max(a,b)
    }

    def comb(a: Int, b: Int) : Int = {
      a + b
    }

    rdd3.aggregateByKey(1)(seq, comb).foreach(println)
  }

  /**
    * hello world1
      hello world2
      hello world3
      hello world4

    */

}
