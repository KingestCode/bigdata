package com.rox.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyDemo1 {

  val conf = new SparkConf()
  conf.setAppName("AggregateByKeyDemo1")
  conf.setMaster("local[4]");
  val sc = new SparkContext(conf)
  val rdd1 = sc.textFile("/Users/shixuanji/Documents/IDEs/iTerm2/scala/a.txt", 4)
  val rdd2 = rdd1.flatMap(_.split(" "))
  val rdd3 = rdd2.map((_, 1))

  def seq(a : Int, b : Int) : Int = {
    Math.max(a,b)
  }

  def comb(a: Int, b: Int) : Int = {
    a + b
  }


  rdd3.aggregateByKey(3)(seq, comb).foreach(println)

}
