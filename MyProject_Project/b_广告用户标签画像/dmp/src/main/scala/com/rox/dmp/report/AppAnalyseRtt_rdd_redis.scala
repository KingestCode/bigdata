package com.rox.dmp.report

import com.rox.dmp.Beans.Log
import com.rox.dmp.utils.{JedisPools, RptUtils}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object AppAnalyseRtt_rdd_redis {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println(
        """
          |com.rox.dmp.report.AppAnalyseRtt_rdd_redis
          |参数：
          | logInputPath 输入路径
          | resultOutputPath 输出路径
        """.stripMargin)
      sys.exit()
    }

    val Array(logInputPath, resultOutputPath) = args


    // 2 创建sparkconf->sparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[Log]))

    val sc = new SparkContext(sparkConf)

    sc.textFile(logInputPath)
      .map(_.split(","))
      .filter(_.length >= 85)
      .map(Log(_))
      .filter(log => !(log.appname.isEmpty && log.appid.isEmpty)) //过滤掉 appname 和 appid 同时为空的
      // Return a new RDD by applying a function to each partition of this RDD.
      .mapPartitions(itr => {
        // 迭代器 => 迭代器
        val jedis: Jedis = JedisPools.getConn()

        //创建一个可迭代的 List 对象, 最后返回此 list, list中是 (appname, List[Double])元祖
        val listBuffer = new collection.mutable.ListBuffer[(String, List[Double])]()

        // 遍历分区的所有数据, 如果 appname 为空的, 就从 redis 中, 根据 appid 找出 redisname
        itr.foreach(log => {
          var newAppName: String = log.appname

          if (newAppName.isEmpty) {
            // 从 redis 中查找 appname
            newAppName = jedis.get(log.appid)
          }
          // 老一套, 找到需要的字段
          val reqList: List[Double] = RptUtils.calculateReq(log.requestmode, log.processnode)
          val rtbList: List[Double] = RptUtils.calculateRtb(log.iseffective, log.isbilling, log.isbid, log.adorderid, log.iswin, log.winprice, log.adpayment)
          val showClickList: List[Double] = RptUtils.calculateShowClick(log.requestmode, log.iseffective)
          // 拼接成元祖, 存入 listbuffer
          listBuffer += ((newAppName, reqList ++ reqList ++ rtbList))

        })
        jedis.close()
        // 将 listBuffer 转为迭代器返回
        listBuffer.iterator
    })
      // zip 拼接值
      .reduceByKey((l1, l2)=> {
      l1.zip(l2).map(t => t._1 + t._2)
    })
      // 调整格式
      .map(t => t._1 + "," + t._2.mkString(","))
      .saveAsTextFile(resultOutputPath)

  }

}
