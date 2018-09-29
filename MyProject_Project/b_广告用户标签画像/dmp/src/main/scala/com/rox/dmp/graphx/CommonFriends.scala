package com.rox.dmp.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CommonFriends {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("共同好友")
    conf.set("spark.rdd.compress", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    // 点
    val uVertices: RDD[(VertexId, String)] = sc.parallelize(Seq(
      (1, ""), (2, ""), (6, ""), (9, ""), (133, ""),
      (16, ""), (44, ""), (21, ""), (138, ""),
      (5, ""), (158, ""), (7, "")
    ))


    // 边
    val uEdges: RDD[Edge[Int]] = sc.parallelize(Seq(
      // 最后一个参数为: 边的有向性, 比如:a 是 b 的表妹,  b 是 a 的表哥; 主要是看是否关注 有向性
      Edge(1, 133, 0),
      Edge(2, 133, 0),
      Edge(6, 133, 0),
      Edge(9, 133, 0),
      Edge(16, 138, 0),
      Edge(21, 138, 0),
      Edge(44, 138, 0),
      Edge(6, 138, 0),
      Edge(5, 158, 0),
      Edge(7, 158, 0)
    ))


    // 图
    val graph = Graph(uVertices,uEdges)

    // 从图中找到可以联通的图的分支，相同分支中的顶点数据向该分支中最小的顶点进行聚合
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    /**
    (44,1)
    (158,5)
    (1,1)
    (138,1)
    (7,5)
    (16,1)
    (21,1)
    (133,1)
    (5,5)
    (2,1)
    (9,1)
    (6,1)
      */

    vertices.map(t => (t._2,t._1.toString)).reduceByKey((v1,v2) => {
      v1+","+v2
    }).foreach(println)

    /**
      * (5,5,158,7)
(1,16,1,9,138,2,44,21,133,6)
      */

    sc.stop()

  }
}
