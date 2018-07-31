package com.rox.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object GroupTop3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Top3").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("file:///Volumes/Selfuser2/北风网/北风网-07spark\\ 从入门/000.课程代码+软件包/第40讲-Spark核心编程：高级编程之topn/文档/score.txt")

    val arrRDD = lines.map(_.split(" ")).cache()
    val pairsRDD = arrRDD.map(e => (e(0), e(1).toInt))
    val groupPairs  = pairsRDD.groupByKey()

    // 注意此处 sortWith 的用法
    // 把迭代器转为数组, 然后用 sortWith算子从大到小排列, 取出前3个
    val top3Score = groupPairs.map(e => {
      e._1 -> e._2.toList.sortWith(_>_).take(3)
    })

    // 第二个参数默认是升序, false 是降序, true 是升序, 按照组名排序
    // 这个就跟 sortByKey 是一样的了
    val groupKeySorted =  top3Score.sortBy(e=>e._1, true)

//    val groupKeySorted = top3Score.sortByKey()

    groupKeySorted.collect().foreach(e => {
      println(e._1 + ":")
      e._2.foreach(println)
      println("----")
    })
  }
}
