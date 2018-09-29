package com.rox.dmp.utils

import org.apache.spark.sql.Row

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object RptCaculate {

  /**
    * !(v(v or !cc or !dd =  !((v(v && cc && dd)
    * 存在 userid
    */
  val hasUserIdCondition =
    """
      |imei != "" or idfa != "" or mac !="" or androidid !="" or openudid != "" or
      |imeimd5 != "" or idfamd5 != "" or macmd5 !="" or androididmd5 !="" or openudidmd5 != "" or
      |imeisha1 != "" or idfasha1 != "" or macsha1 !="" or androididsha1 !="" or openudidsha1 != ""
    """.stripMargin


  /**
    * 拿到 所有的user 标识
    * @param v
    * @return
    */
  def getUserId(v :Row): ListBuffer[String] ={
    var ab = new ListBuffer[String]()

    if (v.getAs[String]("imei").nonEmpty) ab.append("IM:"+(v.getAs[String]("imei").toUpperCase))
    if (v.getAs[String]("idfa").nonEmpty) ab.append("IF:"+(v.getAs[String]("idfa").toUpperCase))
    if (v.getAs[String]("mac").nonEmpty) ab.append("MC:"+(v.getAs[String]("mac").toUpperCase))
    if (v.getAs[String]("androidid").nonEmpty) ab.append("AD:"+(v.getAs[String]("androidid").toUpperCase))
    if (v.getAs[String]("openudid").nonEmpty) ab.append("OD:"+(v.getAs[String]("openudid").toUpperCase))
    if (v.getAs[String]("imeimd5").nonEmpty) ab.append("IMM:"+(v.getAs[String]("imeimd5").toUpperCase))
    if (v.getAs[String]("idfamd5").nonEmpty) ab.append("IFM:"+(v.getAs[String]("idfamd5").toUpperCase))
    if (v.getAs[String]("macmd5").nonEmpty) ab.append("MCM:"+(v.getAs[String]("macmd5").toUpperCase))
    if (v.getAs[String]("androididmd5").nonEmpty) ab.append("ADM:"+(v.getAs[String]("androididmd5").toUpperCase))
    if (v.getAs[String]("openudidmd5").nonEmpty) ab.append("ODM:"+(v.getAs[String]("openudidmd5").toUpperCase))
    if (v.getAs[String]("imeisha1").nonEmpty) ab.append("IMS:"+(v.getAs[String]("imeisha1").toUpperCase))
    if (v.getAs[String]("idfasha1").nonEmpty) ab.append("IFS:"+(v.getAs[String]("idfasha1").toUpperCase))
    if (v.getAs[String]("macsha1").nonEmpty) ab.append("MCS:"+(v.getAs[String]("macsha1").toUpperCase))
    if (v.getAs[String]("androididsha1").nonEmpty) ab.append("ADS:"+(v.getAs[String]("androididsha1").toUpperCase))
    if (v.getAs[String]("openudidsha1").nonEmpty) ab.append("ODS:"+(v.getAs[String]("openudidsha1").toUpperCase))

    ab
  }


}
