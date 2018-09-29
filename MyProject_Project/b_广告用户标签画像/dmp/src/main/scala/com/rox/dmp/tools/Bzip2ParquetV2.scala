package com.rox.dmp.tools

import com.rox.dmp.Beans.Log
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object Bzip2ParquetV2 {

  def main(args: Array[String]): Unit = {


    // 0 校验参数个数
    if (args.length != 3) {
      println(
        """
          |com.rox.dmp.tools.Bzip2Parquet2
          |参数:
          | logInputPath
          | compressionCode <snappy, gzip, lzo>
          | resultOutputPath
        """.stripMargin
      )
      sys.exit()
    }

    // 1 接受程序参数
    val Array(logInputPath, compressionCode, resultOutputPath) = args
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

    // spark2.x 默认是 snappy
    sqlContext.setConf("spark.sql.parquet.compression.codec", compressionCode)


    // 3 读取日志数据 (读取 gbk 数据用下面一种)
    val rawdata: RDD[String] = sc.textFile(logInputPath)

    //    val rawdata: RDD[String] = sc.hadoopFile(logInputPath, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 1).map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))


    // 4 根据数据需求对数据进行 ETL
    val rdd1: RDD[Array[String]] = rawdata.map(line => line.split(",", -1))
      .filter(arr => {
        var b = true
        arr.foreach(str => {
          if (str.length == 0) {
            b = false
          }
        })
        arr.length >= 6 && b
      }).map(arr => {
      arr(4) = arr(4).split(" ")(0)
      arr
    })

    val dataLog: RDD[Log] = rdd1.map(arr => Log(arr))

    val dataFrame: DataFrame = sqlContext.createDataFrame(dataLog)

    dataFrame.show()

    //    dataFrame.createTempView("tmpview")
    //    val addrDF: DataFrame = sqlContext.sql("select addr from tmpview")


    // 可以按照指定字段分区(对象中的字段)
    // 示例字段city 是 provincename 的二级目录
    dataFrame.write.partitionBy("province", "city").parquet(resultOutputPath)
    //    dataFrame.write.csv(resultOutputPath)

    sc.stop()
  }
}
