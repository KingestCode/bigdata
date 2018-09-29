package com.rox.dmp.report

import java.util.Properties

import com.rox.dmp.Beans.Log
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * 把结果写入到 mysql
  */
object ProCityRptV2_sql_mysql {
  def main(args: Array[String]): Unit = {
    // 0 校验参数个数
    if (args.length != 1) {
      println(
        """
          |com.rox.dmp.tools.ProCityRptV2
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
    sparkConf.registerKryoClasses(Array(classOf[Log]))

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    // 3 读取日志数据 (从 parquet 文件中读取)
    val dataFrame: DataFrame = sqlContext.read.parquet(logInputPath)

    //创建临时表
    dataFrame.registerTempTable("log")

    dataFrame.show()

    // 按照省市进行分组聚合--> 统计分组后的各省市的日志记录条数, 并按照省排序
    val dfResult: DataFrame = sqlContext.sql("select province, city, count(*) ct from log group by province, city order by province")

    dfResult.show()


    // 加载配置文件 application.conf -> application.json --> application.properties
    val load: Config = ConfigFactory.load()

    val properties = new Properties()
    properties.setProperty("user",load.getString("jdbc.user"))
    properties.setProperty("password",load.getString("jdbc.password"))

    // 通过 jdbc 写到数据库,
    dfResult.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),properties)

    sc.stop()

  }
}
