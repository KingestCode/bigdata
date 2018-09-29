package com.rox.dmp.tags

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object Tag4Business extends Tags {
  /**
    * 定义打标签的方法接口
    *
    * @param args 任意类型 可变参数
    * @return 返回 map : String -> Int
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()

    val row: Row = args(0).asInstanceOf[Row]
    val jedis: Jedis = args(1).asInstanceOf[Jedis]
    val lat: Double = row.getAs[String]("lat").toDouble
    val longs: Double = row.getAs[String]("longi").toDouble

    if (lat >= 3.86 && lat <= 53.55 && longs >= 73.66 && longs <= 135.05) {
      println("lat: "+lat + "-----longs: "+longs)
      val geoCode = GeoHash.withCharacterPrecision(lat, longs, 8).toBase32
      val business: String = jedis.get(geoCode)
      if (StringUtils.isNotEmpty(business))
        println("business---"+ business)
        business.split(";").foreach(bs => map += "BS"+bs -> 1)
    }
    map
  }
}
