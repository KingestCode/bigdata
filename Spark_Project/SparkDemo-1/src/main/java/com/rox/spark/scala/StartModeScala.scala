package com.rox.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object StartModeScala {
  def main(args: Array[String]): Unit = {
    //创建Spark配置对象
    val conf = new SparkConf()
    conf.setAppName("WordCountSource")

    //设置master属性 ([*]: cpu 的核数)
    conf.setMaster("local[*]");

    //通过conf创建sc
    val sc = new SparkContext(conf)

    // 并行创建 rdd 数据集
    val rdd1 = sc.parallelize(1 to 20)

    // 再分区为4个
    val rdd3 = rdd1.repartition(4)

    rdd3.foreach(e => {
      val tname = Thread.currentThread().getName
      println(tname + ":" + e)
      e * 2
    })
    rdd3.collect()
  }

}
