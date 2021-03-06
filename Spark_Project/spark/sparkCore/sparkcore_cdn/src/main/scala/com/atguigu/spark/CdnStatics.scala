package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.util.matching.Regex

/**
  * Created by wuyufei on 31/07/2017.
  *
  111.19.97.15 HIT 18 [15/Feb/2017:00:00:39 +0800] "GET http://cdn.v.abc.com.cn/videojs/video-js.css HTTP/1.1" 200 14727 "http://www.zzqbsm.com/" "Mozilla/5.0+(Linux;+Android+5.1;+vivo+X6Plus+D+Build/LMY47I)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Version/4.0+Chrome/35.0.1916.138+Mobile+Safari/537.36+T7/7.4+baiduboxapp/8.2.5+(Baidu;+P1+5.1)"

  183.249.134.252 HIT 0 [15/Feb/2017:00:28:38 +0800] "GET http://cdn.v.abc.com.cn/141011.mp4 HTTP/1.1" 206 960 "-" "AppleCoreMedia/1.0.0.14D27+(iPhone;+U;+CPU+OS+10_2_1+like+Mac+OS+X;+zh_cn)"
  *
  * IP 命中率 响应时间 请求时间 请求方法 请求 URL 请求协议 状态吗 响应大小 referer 用户代理
  */
object CdnStatics {

  val logger = LoggerFactory.getLogger(CdnStatics.getClass)

  //匹配IP地址
  val IPPattern = "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))".r

  //匹配视频文件名
  val videoPattern = "([0-9]+).mp4".r

  //[15/Feb/2017:11:17:13 +0800]  匹配 2017:11 按每小时播放量统计
  val timePattern = ".*(2017):([0-9]{2}):[0-9]{2}:[0-9]{2}.*".r

  //匹配 http 响应码和请求数据大小
  val httpSizePattern = ".*\\s(200|206|304)\\s([0-9]+)\\s.*".r

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CdnStatics")

    val sc = new SparkContext(conf)

    val input = sc.textFile("file:///Users/shixuanji/Documents/Code/MyGitSync/Spark_Project/spark/sparkCore/sparkcore_cdn/src/main/resources/cdn.txt").cache()

    //统计独立IP访问量前10位
    ipStatics(input)

    //统计每个视频独立IP数
    videoIpStatics(input)

    //统计一天中每个小时间的流量
    flowOfHour(input)

    sc.stop()
  }

  //统计一天中每个小时间的流量
  def flowOfHour(data: RDD[String]): Unit = {

    def isMatch(pattern: Regex, str: String) = {
      str match {
        case pattern(_*) => true
        case _ => false
      }
    }

    /**
      * 获取日志中小时和http 请求体大小
      *
      * @param line
      * @return
      */
    def getTimeAndSize(line: String) = {
      var res = ("", 0L)
      try {
        val httpSizePattern(code, size) = line
        val timePattern(year, hour) = line
        res = (hour, size.toLong)
      } catch {
        case ex: Exception => ex.printStackTrace()
      }
      res
    }

    //3.统计一天中每个小时间的流量
    data.filter(x=>isMatch(httpSizePattern,x)).filter(x=>isMatch(timePattern,x)).map(x=>getTimeAndSize(x)).groupByKey()
      .map(x=>(x._1,x._2.sum)).sortByKey().foreach(x=>println(x._1+"时 CDN流量="+x._2/(1024*1024*1024)+"G"))
  }

  // 统计每个视频独立IP数
  def videoIpStatics(data: RDD[String]): Unit = {
    def getFileNameAndIp(line: String) = {
      (videoPattern.findFirstIn(line).mkString, IPPattern.findFirstIn(line).mkString)
    }
    //2.统计每个视频独立IP数
    data.filter(x => x.matches(".*([0-9]+)\\.mp4.*")).map(x => getFileNameAndIp(x)).groupByKey().map(x => (x._1, x._2.toList.distinct)).
      sortBy(_._2.size, false).take(10).foreach(x => println("视频：" + x._1 + " 独立IP数:" + x._2.size))
  }


  // 统计独立IP访问量前10位
  def ipStatics(data: RDD[String]): Unit = {

    //1.统计独立IP数
    val ipNums = data.map(x => (IPPattern.findFirstIn(x).get, 1)).reduceByKey(_ + _).sortBy(_._2, false)

    //输出IP访问数前量前10位
    ipNums.take(10).foreach(println)

    println("独立IP数：" + ipNums.count())
  }

}