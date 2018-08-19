package com.rox.mongospark

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

object RDDDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("RDDDemo")
      .setMaster("local[*]")
      .set("spark.mongodb.input.uri","mongodb://ap:123@192.168.170.131:27017/bike.logs")
      .set("spark.mongodb.output.uri","mongodb://ap:123@192.168.170.131:27017/bike.result")

    val sc: SparkContext = new SparkContext(conf)

    val docsRDD: MongoRDD[Document] = MongoSpark.load(sc)

    val pv: Long = docsRDD.count()

    val uv: Long = docsRDD.map(doc => {
      doc.getString("openid")
    }).distinct().count()

    println("pv: " + pv + ", uv: " + uv)

    sc.stop()

  }

}
