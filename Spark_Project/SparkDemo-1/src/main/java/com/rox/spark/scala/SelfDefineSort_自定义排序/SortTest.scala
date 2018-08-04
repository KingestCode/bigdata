package com.rox.spark.scala.SelfDefineSort_自定义排序

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 自定义排序规则排序
  * 1> 把自定义排序规则对象为 key, 原数据为 value
  * 2> 按照
  *
  */
object SortTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SortTest").setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("file:///Volumes/Selfuser2/北风网/北风网-07spark\\ 从入门/000.课程代码+软件包/第39讲-Spark核心编程：高级编程之二次排序/文档/sort.txt", 1)

    val pairs: RDD[(SecondSordKey, String)] = lines.map(line => {
      // 这里只是创建 SecondSordKey 对象 和 原来行的映射
      new SecondSordKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt) -> line
    })

    // 这里才是真正的 sort, 但是会调用自己实现的 SecondSordKey 中的排序方法
    // 每行的数据没有变化, 只是行的位置变了
    val sortedPairs = pairs.sortByKey()

//    val sortedLines = sortedPairs.map(sortedPairs => (sortedPairs._1.first,sortedPairs._1.second, sortedPairs._2))

    /**
      * (1,3,1 3)
        (1,5,1 5)
        (2,1,2 1)
        (2,4,2 4)
        (3,6,3 6)
      */

    val sortedLines = sortedPairs.map(sortedPairs => (sortedPairs._2))

    sortedLines.collect().foreach(println)

  }
}
