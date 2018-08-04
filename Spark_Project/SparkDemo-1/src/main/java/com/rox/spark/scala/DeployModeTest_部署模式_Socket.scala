package com.rox.spark.scala

import java.net.{InetAddress, Socket}

import org.apache.spark.{SparkConf, SparkContext}



object DeployModeTest_部署模式_Socket {

  /**
    * 从运行代码的的节点, 通过套接字发送到 cs5 上
    *
    * @param str
    */
  def printInfo(str:String):Unit = {

    val ip = InetAddress.getLocalHost.getHostAddress
    val sock = new Socket("cs5",8888)
    val out = sock.getOutputStream
    out.write((ip + "" + str + "\r\n").getBytes())
    out.flush()
    sock.close()
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("DeployModeTest")
    // standalone 模式
    //conf.setMaster("spark://cs1:7077")

    // Yarn模式
    conf.setMaster("yarn")

    // local 模式
    // conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    printInfo("hello guys---我就是Driver")


    /**
      * Distribute a local Scala collection to form an RDD.
      * numSlices: the partition number of the new RDD.
      */
    val rdd1 = sc.parallelize(1 to 10,  3)

    // 打印第一次map
    val rdd2 = rdd1.map(e => {
      printInfo("直接定义3个分区, map1: "+e)
      e * 2
    })

    // 重新分区为 2
    val rdd3 = rdd2.repartition(2)

    //
    val rdd4 = rdd3.map(e => {
      printInfo("重分区为2后, map2: " + e)
      e
    })

    val res = rdd4.reduce((a,b)=>{
      printInfo("求和, reduce: " + a + "," + b)
      a + b
    })

    println(res)
    printInfo("最后发给driver: " + res)
  }
}
