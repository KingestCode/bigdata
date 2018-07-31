package com.rox.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 自定义排序规则排序
  */
object SortTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SortTest").setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("file:///Volumes/Selfuser2/北风网/北风网-07spark\\ 从入门/000.课程代码+软件包/第39讲-Spark核心编程：高级编程之二次排序/文档/sort.txt", 1)

    val pairs = lines.map(line => {
      new SecondSordKey(line.split(" ")(0).toInt,line.split(" ")(1).toInt) -> line
    })

    val sortedPairs = pairs.sortByKey()
    val sortedLines = sortedPairs.map(sortedPairs => sortedPairs._2)

    sortedLines.collect().foreach(println)

  }
}
