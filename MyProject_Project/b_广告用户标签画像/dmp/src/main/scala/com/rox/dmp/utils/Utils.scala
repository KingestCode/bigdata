package com.rox.dmp.utils

import org.apache.spark.sql.Row

import scala.collection.mutable

object Utils {

  /**
    * 过滤出经度和纬度在中国境内的数据
    */
  //  val chinaMapCondition = "toDoublePlus(lat) >= 3.86 and toDoublePlus(lat) <= 53.55 " +
  //    "and toDoublePlus(longi)>=73.66 and toDoublePlus(longi)<=135.05"

  val chinaMapCondition = "lat >= 3.86 and lat <= 53.55 " +
    "and longi >= 73.66 and longi <= 135.05"



  /**
    * 获取用户所有不为空的标识
    * @param v
    * @return
    */
  def getUserAllIdPlus(v: Row): mutable.HashMap[String, Int] = {// 守卫

    val set = mutable.HashMap[String, Int]()

    if(v.getAs[String]("imei").nonEmpty         ) set += (("IM#"+v.getAs[String]("imei").toUpperCase(),0))
    if(v.getAs[String]("idfa").nonEmpty         ) set +=(("ID#"+v.getAs[String]("idfa").toUpperCase(),0))
    if(v.getAs[String]("mac").nonEmpty          ) set +=(("MC#"+v.getAs[String]("mac").toUpperCase(),0))
    if(v.getAs[String]("androidid").nonEmpty    ) set +=(("AD#"+v.getAs[String]("androidid").toUpperCase(),0))
    if(v.getAs[String]("openudid").nonEmpty     ) set +=(("OD#"+v.getAs[String]("openudid").toUpperCase(),0))

    if(v.getAs[String]("imeimd5").nonEmpty      ) set +=(("IMM#"+v.getAs[String]("imeimd5").toUpperCase(),0))
    if(v.getAs[String]("idfamd5").nonEmpty      ) set +=(("IDM#"+v.getAs[String]("idfamd5").toUpperCase(),0))
    if(v.getAs[String]("macmd5").nonEmpty       ) set +=(("MCM#"+v.getAs[String]("macmd5").toUpperCase(),0))
    if(v.getAs[String]("androididmd5").nonEmpty ) set +=(("ADM#"+v.getAs[String]("androididmd5").toUpperCase(),0))
    if(v.getAs[String]("openudidmd5").nonEmpty  ) set +=(("ODM#"+v.getAs[String]("openudidmd5").toUpperCase(),0))

    if(v.getAs[String]("imeisha1").nonEmpty     ) set +=(("IMS#"+v.getAs[String]("imeisha1").toUpperCase(),0))
    if(v.getAs[String]("idfasha1").nonEmpty     ) set +=(("IDS#"+v.getAs[String]("idfasha1").toUpperCase(),0))
    if(v.getAs[String]("macsha1").nonEmpty      ) set +=(("MCS#"+v.getAs[String]("macsha1").toUpperCase(),0))
    if(v.getAs[String]("androididsha1").nonEmpty) set +=(("ADS#"+v.getAs[String]("androididsha1").toUpperCase(),0))
    if(v.getAs[String]("openudidsha1").nonEmpty ) set +=(("ODS#"+v.getAs[String]("openudidsha1").toUpperCase(),0))
    set
  }


}
