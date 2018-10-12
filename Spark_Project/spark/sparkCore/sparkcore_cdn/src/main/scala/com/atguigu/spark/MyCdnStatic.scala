package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.util.matching.Regex

object MyCdnStatic {

  val logger = LoggerFactory.getLogger(MyCdnStatic.getClass)

  //匹配IP地址
  private val IPPattern: Regex = "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))".r

  //匹配视频文件名
  val videoPattern = "([0-9]+).mp4".r

  //[15/Feb/2017:11:17:13 +0800]  匹配 2017:11 按每小时播放量统计, 以:分隔,第1为是年, 第2为是 hour
  val timePattern = ".*(2017):([0-9]{2}):[0-9]{2}:[0-9]{2}.*".r

  //匹配 http 响应码和请求数据大小('\s'是空白字符)
  val httpSizePattern = ".*\\s(200|206|304)\\s([0-9]+)\\s.*".r


  ///////////////////////////////////////////////////////////////
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(s"${this.getClass.getSimpleName}")

    val sc = new SparkContext(conf)

    val input: RDD[String] = sc.textFile("file:///Users/shixuanji/Documents/Code/MyGitSync/Spark_Project/spark/sparkCore/sparkcore_cdn/src/main/resources/cdn.txt").cache()

    //统计独立 ip 访问量前10
    //    ipStatic(input)

    //统计每个视频独立 ip 数
    //    videoIpStatic(input)

    //统计每个每个小时的流量
    flowOfHour(input)


    sc.stop()

  }

  /**
    * 计算总的独立 ip 数
    *
    * @param input
    */
  def ipStatic(input: RDD[String]): Unit = {
    //1.直接匹配后再拼1
    //2.累加次数
    //3.按照第二个元素排序(注意不是 sortByKey), 降序排序
    //注意:  使用 get 必须值 notnull
    val ipNums: RDD[(String, Int)] = input
      .map(x => (IPPattern.findFirstIn(x).get, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    ipNums.take(10).foreach(println)
    println("独立 ip 数" + ipNums.count())
  }

  /**
    * 计算每个视频的独立 ip 数
    *
    * @param input
    */
  def videoIpStatic(input: RDD[String]): Unit = {
    //1.过滤出所有包含有.mp4条目的内容---- '.*'表示所有内容, 过滤出的所有数据都是包含 mp4的
    //2.计算出(视频, ip)
    //3.按照视频分组, ip 转为 list 去重
    //4.计算 list.size 即可
    def getFileNameAndIp(line: String) = {
      (videoPattern.findFirstIn(line).get, IPPattern.findFirstIn(line).get)
    }

    input.filter(x => x.matches(".*([0-9]+)\\.mp4.*")).map(x => getFileNameAndIp(x)).groupByKey()
      .map(x => (x._1, x._2.toList.distinct))
      .sortBy(_._2.size, false)
      .take(10).foreach(x => println("视频: " + x._1 + ", 独立ip 数: " + x._2.size))
  }


  /**
    * 计算每小时的流量
    *
    * @param input
    */
  def flowOfHour(input: RDD[String]): Unit = {

    //传入字符串是否匹配正则
    def isMatch(pattern: Regex, str: String) = {
      str match {
        case pattern(_*) => true
        case _ => false
      }
    }

    // 获取日志中 小时数 和http 请求体大小
    def getTimeAndSize(line: String) = {
      var res = ("", 0L)
      try {
        val httpSizePattern(code, size) = line
        val timePattern(year, hour) = line
        res = (hour, size.toLong)
      } catch {
        case ex: Exception => ex.printStackTrace()
      }
      res    //1.首先过滤掉不符合 时间规则 & http,size 规则的数据
      //2.获取 (小时,流量)
      //3.按照小时分组
      //4.流量求和, 按照小时排序, 顺序, 1个partition 聚合
      //5.流量换算为 GB, 原本是B
      input.filter(x => isMatch(httpSizePattern,x))
        .filter(x => isMatch(timePattern,x))
        .map(x => getTimeAndSize(x))
        .groupByKey()
        .map(x => (x._1, x._2.sum))
        .sortByKey(true,1)
        .foreach(x => println(x._1+"时 CDN流量="+x._2/(1024*1024*1024)+"GB"))

    }


  }


}
