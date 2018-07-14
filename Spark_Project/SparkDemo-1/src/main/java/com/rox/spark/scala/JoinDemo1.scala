package com.rox.spark.scala

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Join 连接: 自会动对相同的 k 进行连接
  * (K,V).join(K,W) => (K,(V,W))
  *
  * When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key. Outer joins are supported through leftOuterJoin, rightOuterJoin, and fullOuterJoin.
  */
object JoinDemo1 {

  def main(args: Array[String]): Unit = {

    var conf = new SparkConf()
    conf.setAppName("JoinDemo1")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    // 加载names
    val names_rdd1 = sc.textFile("/Users/shixuanji/Documents/IDEs/iTerm2/scala/names")
    // 映射出一个元祖对偶, 拿到返回值
    val names_rdd2 = names_rdd1.map(line => {
      val arr = line.split(" ")
      (arr(0).toInt,arr(1))
    })

    // 加载scores
    val scores_rdd1 = sc.textFile("/Users/shixuanji/Documents/IDEs/iTerm2/scala/scores")
    // 映射出一个元祖对偶, 拿到返回值
    val scores_rdd2 = scores_rdd1.map(line => {
      val arr = line.split(" ")
      (arr(0).toInt, arr(1).toInt)
    })

    // join() 会自动对相同的 key 进行 join
    val join_rdd = names_rdd2.join(scores_rdd2)

    // 搜集, 遍历
    join_rdd.collect().foreach(t => {
      println(t._1 + " : " + t._2)
    })

    /**
      * 结果:
      * 4 : (tomson,600)
        1 : (tom,600)
        3 : (tomasLee,450)
        2 : (tomas,580)
      */

  }

}
