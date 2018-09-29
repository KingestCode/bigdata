package com.rox.dmp.tools

import com.rox.dmp.utils.{SStringUtils, SchemaUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object ToParquent_v1 {
  def main(args: Array[String]): Unit = {


    // 0 校验参数个数
    if (args.length != 3) {
      println(
        """
          |cn.dmp.tools.Bzip2Parquet
          |参数：
          | logInputPath
          | compressionCode <snappy, gzip, lzo>
          | resultOutputPath
        """.stripMargin)
      sys.exit()
    }

    // 1 接受程序参数
    val Array(logInputPath, compressionCode,resultOutputPath) = args

    // 2 创建sparkconf->sparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)

    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec", compressionCode)


    // 3 读取日志数据
    val rawdata = sc.textFile(logInputPath)

    // 4 根据业务需求对数据进行ETL  xxxx,x,x,x,x,,,,,
    val dataRow: RDD[Row] = rawdata
      .map(line => line.split(",", line.length))
      .filter(_.length >= 85)
      .map(arr => {
        Row(
          arr(0),
          SStringUtils.toInt(arr(1)),
          SStringUtils.toInt(arr(2)),
          SStringUtils.toInt(arr(3)),
          SStringUtils.toInt(arr(4)),
          arr(5),
          arr(6),
          SStringUtils.toInt(arr(7)),
          SStringUtils.toInt(arr(8)),
          SStringUtils.toDouble(arr(9)),
          SStringUtils.toDouble(arr(10)),
          arr(11),
          arr(12),
          arr(13),
          arr(14),
          arr(15),
          arr(16),
          SStringUtils.toInt(arr(17)),
          arr(18),
          arr(19),
          SStringUtils.toInt(arr(20)),
          SStringUtils.toInt(arr(21)),
          arr(22),
          arr(23),
          arr(24),
          arr(25),
          SStringUtils.toInt(arr(26)),
          arr(27),
          SStringUtils.toInt(arr(28)),
          arr(29),
          SStringUtils.toInt(arr(30)),
          SStringUtils.toInt(arr(31)),
          SStringUtils.toInt(arr(32)),
          arr(33),
          SStringUtils.toInt(arr(34)),
          SStringUtils.toInt(arr(35)),
          SStringUtils.toInt(arr(36)),
          arr(37),
          SStringUtils.toInt(arr(38)),
          SStringUtils.toInt(arr(39)),
          SStringUtils.toDouble(arr(40)),
          SStringUtils.toDouble(arr(41)),
          SStringUtils.toInt(arr(42)),
          arr(43),
          SStringUtils.toDouble(arr(44)),
          SStringUtils.toDouble(arr(45)),
          arr(46),
          arr(47),
          arr(48),
          arr(49),
          arr(50),
          arr(51),
          arr(52),
          arr(53),
          arr(54),
          arr(55),
          arr(56),
          SStringUtils.toInt(arr(57)),
          SStringUtils.toDouble(arr(58)),
          SStringUtils.toInt(arr(59)),
          SStringUtils.toInt(arr(60)),
          arr(61),
          arr(62),
          arr(63),
          arr(64),
          arr(65),
          arr(66),
          arr(67),
          arr(68),
          arr(69),
          arr(70),
          arr(71),
          arr(72),
          SStringUtils.toInt(arr(73)),
          SStringUtils.toDouble(arr(74)),
          SStringUtils.toDouble(arr(75)),
          SStringUtils.toDouble(arr(76)),
          SStringUtils.toDouble(arr(77)),
          SStringUtils.toDouble(arr(78)),
          arr(79),
          arr(80),
          arr(81),
          arr(82),
          arr(83),
          SStringUtils.toInt(arr(84))
        )
      })


    // 5 将结果存储到本地磁盘
    val dataFrame = sQLContext.createDataFrame(dataRow, SchemaUtils.logStructType)

    dataFrame.show()

    dataFrame.coalesce(1).write.parquet(resultOutputPath)
    // 6 关闭sc
    sc.stop()
  }

}
