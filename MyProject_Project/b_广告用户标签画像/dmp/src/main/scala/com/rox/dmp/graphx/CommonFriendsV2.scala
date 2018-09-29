package com.rox.dmp.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CommonFriendsV2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("共同好友")
    conf.set("spark.rdd.compress", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    // 点
    val uvertices: RDD[(VertexId, (String, String))] = sc.parallelize(Seq(
      (1, ("陈伟", "男")), (2, ("宋忠鑫", "男")), (6, ("姚翔", "女")), (9, ("王振华", "男")), (133, ("孔斌", "女")),
      (16, ("张超", "男")), (44, ("董浩楠", "男")), (21, ("刘东鑫", "男")), (138, ("刘明文", "男")),
      (5, ("陈邵晶", "女")), (158, ("陈俊豪", "男")), (7, ("吴昊", "男"))
    ))

    // 边
    val uedges: RDD[Edge[Int]] = sc.parallelize(Seq(
      Edge(1, 133, 0),
      Edge(2, 133, 0),
      Edge(6, 133, 0),
      Edge(9, 133, 0),
      Edge(16, 138, 0),
      Edge(21, 138, 0),
      Edge(44, 138, 0),
      Edge(6, 138, 0),
      Edge(5, 158, 0),
      Edge(7, 158, 0) // (张三, 男)
    ))


    // 图
    val graph = Graph(uvertices, uedges)

    // 从图中找到可以联通的图的分支，相同分支中的顶点数据向该分支中最小的顶点进行聚合(uid, cmMinId)
    val cc: VertexRDD[VertexId] = graph.connectedComponents().vertices

    /**
      * cc:
      * (44,1)
        (138,1)
        (158,5)
        (6,1)
        (16,1)
        (1,1)
      .....
      * cc: (uId, commonMinId)
      * uvertices: (uId, (name, sex))
      */

//    cc.join(uvertices).foreach(println)
    /**
      * (21,(1,(刘东鑫,男)))
        (1,(1,(陈伟,男)))
        (158,(5,(陈俊豪,男)))
        (44,(1,(董浩楠,男)))
      */

    /* 根据 uid 进行 join */
    cc.join(uvertices)
      /* 组成元祖 (最小顶点值, List(附带多有信息)) */
      .map(t => (t._2._1, List(t._2._2)))
      /* 跟拒 key 进行聚合, 把 List 进行拼接 */
      .reduceByKey(_ ++ _)
      .foreach(println)

    /**
      * 相同的最小顶点后的 List, 就是共同好友
      * (5,List((陈邵晶,女), (陈俊豪,男), (吴昊,男)))
      * (1,List((张超,男), (陈伟,男), (王振华,男), (刘明文,男), (宋忠鑫,男), (董浩楠,男), (刘东鑫,男), (孔斌,女), (姚翔,女)))
      */

    sc.stop()

  }
}
