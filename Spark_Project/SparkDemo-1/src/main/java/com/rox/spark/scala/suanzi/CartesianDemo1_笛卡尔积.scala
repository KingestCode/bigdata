package com.rox.spark.scala.suanzi

import org.apache.spark.{SparkConf, SparkContext}

object CartesianDemo1_笛卡尔积 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("CartesianDemo1")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    // 构建一个 RDD
    val rdd1 = sc.parallelize(Array("tom","tomas","tomasle","tomson"))
    val rdd2 = sc.parallelize(Array("1234","3456","5678","7890"))

    // 笛卡尔积, 全连接
    val rdd = rdd1.cartesian(rdd2)
    rdd.collect().foreach(println)
  }
}
