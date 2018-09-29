package com.rox.dmp.report

import com.rox.dmp.Beans.Log
import com.rox.dmp.utils.RptUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * appname 分析表
  * broadcast 广播变量的使用
  *
  * 媒 体  类     总请求           有效请求         广告请求          参与竞价         竞  价  成 功     竞价成功          展示      点击      点击       广  告  成     广  告  消
  * *
  * 别                                                   数            数             率             量       量       率        本           费
  * *
  * 爱奇艺                                                 1000         600           30.56%        800     800     2%       15.87       15.87
  * *
  * 腾 讯  新                                              1000         600           23.23%        700     323     4%       98.21       50.81
  * *
  * 闻
  * *
  * PPTV                                                1000         600           23.23%        700     323     4%       98.21       50.81
  *
  */
object AppAnalyseRpt_rdd {
  def main(args: Array[String]): Unit = {
    // 0 校验参数个数
    if (args.length != 3) {
      println(
        """
          |com.rox.dmp.tools.ProCityRpt
          |参数:
          | logInputPath
          | appDictPath
          | resultOutputPath
        """.stripMargin
      )
      sys.exit()
    }

    // 1 接受程序参数
    val Array(logInputPath, appDictPath, resultOutputPath) = args

    // 2 创建sparkconf -> sparkcontext
    val sparkConf = new SparkConf()

    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")

    // RDD序列化到磁盘  worker 与 worker 之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 手动注册 自定义类(Log) 的序列化方式, 否则无法使用kryo 序列化
    sparkConf.registerKryoClasses(Array(classOf[Log]))

    val sc = new SparkContext(sparkConf)

    // 最后要从各个节点搜集到 driver 端, 然后转为 map
    val broadcastMap: Map[String, String] = sc.textFile(appDictPath).map(line => {
      line.split("\t",-1)
    }).filter(_.length >= 5)
      .map(arr => (arr(4), arr(1)))
      .collect().toMap

    // 把 map 广播出去
    val broadcast: Broadcast[Map[String, String]] = sc.broadcast(broadcastMap)

    sc.textFile(logInputPath)
      .map(_.split(","))
      .filter(_.length >= 85)
      .map(Log(_))
      .filter(log => !(log.appname.isEmpty && log.appid.isEmpty)) //过滤掉 appname 和 appid 同时为空的
      .map(log => {
        var newAppName = log.appname
        if (newAppName.isEmpty) { //如果 appname 是空的话, 就通过 appid 找 appname, 找不到的话就返回固定值
          newAppName = broadcast.value.getOrElse(log.appid, "APPNAME NOT FOUND!!!")
        }
        // 老一套, 找到需要的字段
        val reqList: List[Double] = RptUtils.calculateReq(log.requestmode, log.processnode)
        val rtbList: List[Double] = RptUtils.calculateRtb(log.iseffective, log.isbilling, log.isbid, log.adorderid, log.iswin, log.winprice, log.adpayment)
        val showClickList: List[Double] = RptUtils.calculateShowClick(log.requestmode, log.iseffective)
        // 拼接成元祖
        (newAppName, reqList ++ reqList ++ rtbList)
    })
      // zip 拼接值
      .reduceByKey((l1, l2)=> {
      l1.zip(l2).map(t => t._1 + t._2)
    })
      // 调整格式
      .map(t => t._1 + "," + t._2.mkString(","))
      .saveAsTextFile(resultOutputPath)
  }
}
