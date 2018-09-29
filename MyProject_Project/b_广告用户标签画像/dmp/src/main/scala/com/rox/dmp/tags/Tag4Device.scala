package com.rox.dmp.tags

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * app名称, 设备
  * 打标签
  */
object Tag4Device extends Tags{
  /**
    * 定义打标签的方法接口
    *
    * @param args 任意类型 可变参数
    * @return 返回 map : String -> Int
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()

    val row: Row = args(0).asInstanceOf[Row]
    val appDict: Map[String, String] = args(1).asInstanceOf[Map[String,String]]

    // 标记 appname
    val appId: String = row.getAs[String]("appid")
    var appName: String = row.getAs[String]("appname")

    if (StringUtils.isEmpty(appName)) {
      appName = appDict.getOrElse(appId,appId)
    }

    if (StringUtils.isNotEmpty(appName)) {
      map += "APP"+appName -> 1
    }

    // 标记设备类型 client: Int, 设备类型 （1：android 2：ios 3：wp）
    val client: Int= row.getAs[Int]("client")
    client match {
      case 1 => map += "D00010001"->1
      case 2 => map += "D00010002"->1
      case 3 => map += "D00010003"->1
      case _ => map += "D00010004"->1
    }


    val nwk = row.getAs[String]("networkmannername")
    nwk.toUpperCase() match  {
      case "WIFI" => map += "D00020001" -> 1
      case "4G" => map += "D00020002" -> 1
      case "3G" => map += "D00020003" -> 1
      case "2G" => map += "D00020004" -> 1
      case _ => map += "D00020005" -> 1
    }


    val isp = row.getAs[String]("ispname")
    isp match  {
      case "移动" => map += "D00030001" -> 1
      case "联通" => map += "D00030002" -> 1
      case "电信" => map += "D00030003" -> 1
      case _ => map += "D00030004" -> 1
    }

    map
  }
}
