package com.rox.spark.scala.Kafka_redis

import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

object CalculateUtil {
  def calculateZone(fields: RDD[Array[String]], broadcastRef: Any) = {


  }

  /**
    * 计算商品分类总金额
    * @param fields
    */
  def calculateItem(fields: RDD[Array[String]]) = {
    println("正在处理 计算商品分类总金额")
    val itemAndPrice: RDD[(String, Double)] = fields.map(arr => {
      // 分类
      val item: String = arr(2)
      // 金额
      val price: Double = arr(4).toDouble
      (item -> price)
    })

    // 按照商品分类进行聚合
    val reduced: RDD[(String, Double)] = itemAndPrice.reduceByKey((x,y)=>x+y)

    //将当前批次的数据累加到Redis中
    //foreachPartition是一个Action
    //现在这种方式，jeids的连接是在哪一端创建的（Driver）
    //在Driver端拿Jedis连接不好, 首先得知道拿到的 conn 有没有序列化, 即使有序列化, 通过网络传输也不好
    //应该直接在 executor 中创建
    //val conn = JedisConnectionPool.getConnection()

    // Applies a function f to each partition of this RDD.
    reduced.foreachPartition(part => {
      //获取一个Jedis连接
      //这个连接其实是在Executor中的获取的
      //JedisConnectionPool在一个Executor进程中只有一个（单例）
      val conn: Jedis = JedisConnectionPool_redis.getConnection()
      part.foreach(t => {
        // 一个连接更新多条数据
        conn.incrByFloat(t._1,t._2)
      })
      conn.close()
    })
  }


  /**
    * 计算成交总额
    * @param fields
    */
  def calculateIncome(fields: RDD[Array[String]]) = {

    println("正在处理 计算成交总额")

    val priceRDD: RDD[Double] = fields.map(arr => {
      val price = arr(4).toDouble
      price
    })

    // reduce()是一个 action, 会把结果返回到 Driver 端
    val sum : Double = priceRDD.reduce(_+_)

    // 获取一个 Jedis 连接
    val conn: Jedis = JedisConnectionPool_redis.getConnection()

    // 在 redis 中讲历史值和当前的值进行累加
    conn.incrByFloat(Constant.TOTAL_INCOME,sum)

    // 释放连接
    // 将数据计算后, 写入到 Redis
    conn.close()
  }


}
