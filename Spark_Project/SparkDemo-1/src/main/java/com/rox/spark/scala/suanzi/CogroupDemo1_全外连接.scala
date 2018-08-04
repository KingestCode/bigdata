package com.rox.spark.scala.suanzi

import org.apache.spark.{SparkConf, SparkContext}

/**
  * (K,V).cogroup(K,W) => (K,(Iterable<V>,Iterable <W>))
  */
object CogroupDemo1_全外连接 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("CogroupDemo1")
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("/Users/shixuanji/Documents/IDEs/iTerm2/scala/cogroup-1")

    val rdd2 = rdd1.map(line => {
      var arr = line.split(" ")
      (arr(0),arr(1))
    })

    val rdd3 = sc.textFile("/Users/shixuanji/Documents/IDEs/iTerm2/scala/cogroup-2")

    val rdd4 = rdd3.map(line => {
      var arr = line.split(" ")
      (arr(0),arr(1).toInt)
    })

    // cogroup, 把相同 key 下的 所有 v 都聚合在一起
    val rdd = rdd2.cogroup(rdd4)

    rdd.collect().foreach(t => {
      println("================="+t._1)
      for (e <- t._2._1){
        println(e)
      }
      for (e <- t._2._2){
        println(e)
      }
    })
  }

  /**
    * 结果:
    * =================hebei
      jerry
      jessica
      jack
      99
      999
      =================henan
      tom
      tomas
      tomasLee
      10
      100
      1000
      10000
    */

}
