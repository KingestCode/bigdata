package com.rox.dmp.tags

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * 广告位, 广告平台商, 地域
  * 打标签
  *
adspacetype: Int,   广告位类型（1：banner 2：插屏 3：全屏）
adspacetypename: String,  广告位类型名称（banner、插屏、全屏）
  */
object Tag4AdSpace extends Tags {
  /**
    * 定义打标签的方法接口
    *
    * @param args 任意类型 可变参数
    * @return 返回 map : String -> Int
    */
  override def makeTags(args: Any*): Map[String, Int] = {

    var map = Map[String,Int]()

    val row: Row = args(0).asInstanceOf[Row]

    val adsType: Int = row.getAs[Int]("adspacetype")
    val adsTypeName: String = row.getAs[String]("adspacetypename")

    // 广告位类型标签
    if (adsType > 9) map += "LC"+adsType -> 1
    if (adsType < 10 && adsType > 0) map += "LC0"+adsType -> 1
    // 广告位类型名称
    if (StringUtils.isNotEmpty(adsTypeName)) map += "LN"+adsTypeName -> 1


    // 广告平台商
    val channel: Int = row.getAs[Int]("adplatformproviderid")
    if (channel > 0) map += "CN"+channel -> 1

    // 地域标签
    val pName: String = row.getAs[String]("provincename")
    val cName: String = row.getAs[String]("cityname")
    if (StringUtils.isNotEmpty(pName)) map += "ZP"+pName -> 1
    if (StringUtils.isNotEmpty(cName)) map += "ZC"+cName -> 1

    map
  }

}
