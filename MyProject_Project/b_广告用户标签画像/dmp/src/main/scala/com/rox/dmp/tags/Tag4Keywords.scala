package com.rox.dmp.tags

import org.apache.spark.sql.Row

/*
  关键字（标签格式：Kxxx->1）xxx 为关键字，关键字个数不能少于 3 个字符，且不能超过 8 个字符；关键字中如包含‘‘|’’，则分割成数组，转化成多个关键字标签

 */
object Tag4Keywords extends Tags {
  /**
    * 定义打标签的方法接口
    *
    * @param args 任意类型 可变参数
    * @return 返回 map : String -> Int
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()

    val row: Row = args(0).asInstanceOf[Row]
    val stopWords: Map[String, Int] = args(1).asInstanceOf[Map[String,Int]]

    val kws: String = row.getAs[String]("keywords")

    kws.split("\\|")
      .filter(t=> t.length >=3 && t.length <=8 && !stopWords.contains(t))
      .foreach(t => map += "K"+t->1)

    map
  }
}
