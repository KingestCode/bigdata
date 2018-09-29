package com.rox.dmp.report

import com.rox.dmp.Beans.Log
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * 本次统计是基于 parquet 文件
  * 需求1:
  *   将统计出来的结果存储为 json 文件格式
  * 需求2:
  *   将统计出来的结果存储到mysql中
  */
object ProCityRpt {

  def main(args: Array[String]): Unit = {

    // 0 校验参数个数
    if (args.length != 2) {
      println(
        """
          |com.rox.dmp.tools.ProCityRpt
          |参数:
          | logInputPath
          | resultOutputPath
        """.stripMargin
      )
      sys.exit()
    }

    // 1 接受程序参数
    val Array(logInputPath, resultOutputPath) = args
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

    // 4 判断结果存储路径是否存在, 如果存在则删除
    val hadoopConfiguration: Configuration = sc.hadoopConfiguration
    val fs: FileSystem = FileSystem.get(hadoopConfiguration)

    val resultPath = new Path(resultOutputPath)
    if (fs.exists(resultPath)) {
      fs.delete(resultPath,true)
    }

    // 5 将结果存储城 json 文件, 并合并为1个分区
    dfResult.coalesce(1).write.json(resultOutputPath)

    sc.stop()
  }

  /**
    *
  +--------+----+---+
|province|city| ct|
+--------+----+---+
|      浙江| 舟山市|  1|
|      安徽| 巢湖市|  1|
|      江苏| 启东市|  2|
|      河南| 郑州市|  1|
|      陕西| 扶风县|  1|
|      福建| 莆田市|  1|
|      海南| 海口市|  2|
|      吉林| 蛟河市|  1|
|      陕西| 岐山县|  1|
|      上海| 宝山区|  1|
|     黑龙江| 大庆市|  2|
|      辽宁| 铁岭市|  1|
|      陕西| 榆林市|  2|
|      河南| 林州市|  1|
|      北京| 石景山|  1|
|      江苏| 南京市|  1|
|      陕西| 泾阳县|  1|
|      上海| 黄浦区|  1|
|      天津| 河西区|  2|
|      上海| 静安区|  1|
+--------+----+---+
    */

}
