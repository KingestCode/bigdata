package com.rox.dmp.report

import java.util.Properties

import com.rox.dmp.Beans.Log
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}


/**
  * 广告投放的地域分布
  * http://p6i5vzkfk.bkt.clouddn.com/study/2018-09-04-133320.jpg
  * 注意: 这里是直接读取 parquet 文件
  *
  */
object AreaAnalyseRpt_sql {

  def main(args: Array[String]): Unit = {

    // 0 校验参数个数
    if (args.length != 1) {
      println(
        """
          |com.rox.dmp.tools.ProCityRpt
          |参数:
          | logInputPath
        """.stripMargin
      )
      sys.exit()
    }

    // 1 接受程序参数
    val Array(logInputPath) = args

    // 2 创建sparkconf -> sparkcontext
    val sparkConf = new SparkConf()

    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")

    // RDD序列化到磁盘  worker 与 worker 之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 手动注册 自定义类(Log) 的序列化方式, 否则无法使用kryo 序列化
//    sparkConf.registerKryoClasses(Array(classOf[Log]))

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // 3 读取日志数据 (从 parquet 文件中读取)
    val dataFrame: DataFrame = sqlContext.read.parquet(logInputPath)

    //创建临时表
    dataFrame.registerTempTable("log")

//    dataFrame.show()

    // 注意: mysql 中尽量不要是用 中文的  title, 如果使用, 使用 ``包起来
    val dfResult = sqlContext.sql(
      """
        |select
        |provincename, cityname,
        |sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) `有效请求`,
        |sum(case when requestmode=1 and processnode =3 then 1 else 0 end) `广告请求`,
        |sum(case when iseffective=1 and isbilling=1 and isbid=1 and adorderid !=0 then 1 else 0 end) `参与竞价数`,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1 else 0 end) `竞价成功数`,
        |sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) `展示数`,
        |sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) `点击数`,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1.0*adpayment/1000 else 0 end) `广告成本`,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1.0*winprice/1000 else 0 end) `广告消费`
        |from log
        |group by provincename, cityname
      """.stripMargin)

    dfResult.show()

    // 加载配置文件 application.conf -> application.json --> application.properties
    val load: Config = ConfigFactory.load()

    val properties = new Properties()
    properties.setProperty("user", load.getString("jdbc.user"))
    properties.setProperty("password", load.getString("jdbc.password"))

    // 通过 jdbc 写到数据库,
    dfResult.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"), load.getString("jdbc.areaRptTable"), properties)

    sc.stop()

  }
}
