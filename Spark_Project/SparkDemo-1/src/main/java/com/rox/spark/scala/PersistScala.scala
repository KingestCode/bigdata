package com.rox.spark.scala

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.tools.nsc.io.Socket

object PersistScala {

  def main(args: Array[String]): Unit = {
    //创建Spark配置对象
    val conf = new SparkConf()
    conf.setAppName("WordCountSource")

    //设置master属性
    conf.setMaster("local");

    //通过conf创建sc
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(1 to 20)

    val rdd2 = rdd1.map(e => {
      println(e)
      e * 2
    })

    /**
      * Persist this RDD with the default storage level (`MEMORY_ONLY`).
      * def cache(): this.type = persist()
      */
    //    rdd2.cache()

    /**
      * 存储位置:
      * /private/var/folders/6l/blvdbwd53hqglz0f09n3sd5c0000gn/T/blockmgr-7211febf-816f-474a-b448-0c1eb7cec47e
      */
    rdd2.persist(StorageLevel.DISK_ONLY)

    /**
      * Gets the name of the directory to which this RDD was checkpointed.
      * This is not defined if the RDD is checkpointed locally.
      */
    rdd2.getCheckpointFile

    println(rdd2.reduce(_ + _))

    // 如果中途把磁盘上存储文件内容删掉了(目录不能删), 下面还会重新算
    // 这里就是删的文件内容
    rdd2.unpersist()

    println(rdd2.reduce(_ + _))

    println(java.net.InetAddress.getLocalHost.getHostAddress + " : " + Thread.currentThread().getName)

    /*var str: String = java.net.InetAddress.getLocalHost.getHostAddress
    str = str + " : " + Thread.currentThread().getName + "\r\n"

    val socket = new java.net.Socket("cs1", 8888)
    val out = socket.getOutputStream

    out.write(str.getBytes())
    out.flush()
    out.close()
    socket.close()*/
  }
}
