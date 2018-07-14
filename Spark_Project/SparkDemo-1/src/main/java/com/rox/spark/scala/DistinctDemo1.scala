package com.rox.spark.scala

/**
  * Returns a new dataset that contains the distinct(截然不同的) elements of the source dataset.
  * 去重, 相同的只留一份,去除重复的元素。
  */
object DistinctDemo1 {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.SparkConf
    import org.apache.spark.SparkContext
    val conf = new SparkConf
    conf.setAppName("WordCountScala")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("/Users/shixuanji/Documents/a.txt", 4)
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.distinct


    rdd3.collect.foreach(println)
  }
}
