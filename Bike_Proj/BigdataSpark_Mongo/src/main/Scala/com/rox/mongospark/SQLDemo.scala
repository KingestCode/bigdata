package com.rox.mongospark

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{DataFrame, SparkSession}

object SQLDemo {
  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SQLDemo")
      .config("spark.mongodb.input.uri", "mongodb://ap:123@192.168.170.131:27017/bike.logs")
      .config("spark.mongodb.output.uri", "mongodb://ap:123@192.168.170.131:27017/bike.results")
      .getOrCreate()

    val df: DataFrame = MongoSpark.load(session)

    df.createTempView("v_logs")

    val pvuv_df: DataFrame = session.sql("select count(*) pv, count(distinct openid) uv from v_logs")

    pvuv_df.show()

    // 保存到 mongo, 路径在前面的 output.uri 中已经指定号了
    MongoSpark.save(pvuv_df)

    session.stop()
  }

}
