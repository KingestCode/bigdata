package com.rox.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object Top3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top3").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("file:///Volumes/Selfuser2/北风网/北风网-07spark\\ 从入门/000.课程代码+软件包/第40讲-Spark核心编程：高级编程之topn/文档/top.txt")

    lines.map(e => (e.toInt, e)).sortByKey().map(_._1).top(3).foreach(println)
  }

}
