package com.rox.dmp.tools

import ch.hsr.geohash.GeoHash
import com.rox.dmp.Beans.Log
import com.rox.dmp.utils.{JedisPools, SStringUtils, Utils}
import com.rox.lbs.baidu.SnCal
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object LatLong2Bussiness {

  private val load = ConfigFactory.load()

  /**
    * 注册 UDF, 转换经纬度数据格式
    *
    * @param str
    * @return
    */
  def latLong2Double(str: String): Double = {
    SStringUtils.toDouble(str)
  }


  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println(
        """
          |com.rox.dmp.tools.LatLong2Bussiness
          |参数:
          | inputPath
        """.stripMargin
      )
      sys.exit()
    }

    val Array(inputPath) = args

    val sparkConf = new SparkConf()

    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")

    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.debug.maxToStringFields", "100") //处理字段过多时, 要设置此项, 默认是25个

    sparkConf.registerKryoClasses(Array(classOf[Log]))

    val sc = new SparkContext(sparkConf)
    val sQLContext = new SQLContext(sc)

    //注册自定义函数 (后面是方法转函数)
    sQLContext.udf.register("toDoublePlus", latLong2Double _)

    sQLContext.read.parquet(inputPath).select("lat", "longi")
      .where(Utils.chinaMapCondition).distinct()
      .foreachPartition(itr => {
        //获取 Jedis 连接
        val jedis: Jedis = JedisPools.getConn()

        itr.foreach(row => {
          val lat: String = row.getAs[String]("lat")
          val longs: String = row.getAs[String]("longi")
          println(lat +","+ longs)

          if (StringUtils.isNotEmpty(lat) && StringUtils.isNotEmpty(longs)){
            val geoHashCode: String = GeoHash.withCharacterPrecision(lat.toDouble, longs.toDouble, 8).toBase32 //获取经纬度的 geoHashCode
            val bussiness: String = SnCal.bussiness(lat + "," + longs) //获取经纬度对应商圈

            if (StringUtils.isNotEmpty(bussiness)){
              // 存入 reids
              jedis.set(geoHashCode, bussiness)
            }
          }
        })

        jedis.close()
      })
  }
}
