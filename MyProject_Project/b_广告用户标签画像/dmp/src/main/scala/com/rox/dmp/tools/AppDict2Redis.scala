package com.rox.dmp.tools

import com.rox.dmp.Beans.Log
import com.rox.dmp.utils.JedisPools
import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object AppDict2Redis {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println(
        """
          |com.rox.dmp.tools.AppDict2Redis
          |参数:
          | appDictPath
        """.stripMargin
      )
      sys.exit()
    }

    // 1 接受程序参数
    val Array(appDictPath) = args

    // 2 创建sparkconf -> sparkcontext
    val sparkConf = new SparkConf()

    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")

    // RDD序列化到磁盘  worker 与 worker 之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 手动注册 自定义类(Log) 的序列化方式, 否则无法使用kryo 序列化
    sparkConf.registerKryoClasses(Array(classOf[Log]))

    val sc = new SparkContext(sparkConf)
    sc.textFile(appDictPath).map(line => {
      line.split("\t",-1)
    }).filter(_.length >= 5)
      .filter(arr => {
            arr(4).startsWith("com.")
        })
      .map(arr => (arr(4), arr(1)))
      // 减少jedis 的连接次数, 所以用 foreachPartition
      // 开启单机版redis, 进入 redis 还要设置下 CONFIG SET protected-mode no
      .foreachPartition(t => {
        val jedis: Jedis = JedisPools.getConn()
        t.foreach(t => {
          jedis.set(t._1,t._2)
        })
        jedis.close()
      })

    sc.stop()

  }

}
