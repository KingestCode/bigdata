package com.rox.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 随机挑选出样本子集
  * Sample a fraction of the data, with or without replacement, using a given random number generator seed.
  * //采样返回采样的RDD子集。
    //withReplacement 元素是否可以多次采样.
    //fraction : 期望采样数量.[0,1]
  */
object SampleDemo1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("SampleDemo1")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("/Users/shixuanji/Documents/a.txt", 4)
    val rdd2 = rdd1.flatMap(_.split(" "))

    // Return a sampled subset of this RDD
    // 第2个参数为, 样本占原数据的比例
    val rdd3 = rdd2.sample(false, 0.5)

    rdd3.collect().foreach(println)
  }

}
