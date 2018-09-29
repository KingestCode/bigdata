package com.rox.dmp.tools

import com.rox.dmp.Beans.Log
import com.rox.dmp.utils.SchemaUtils
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object Bzip2Parquet {

  def main(args: Array[String]): Unit = {

    // 0 校验参数个数
    if (args.length != 4) {
      println(
        """
          |com.rox.dmp.tools.Bzip2Parquet
          |参数:
          | logInputPath
          | compressionCode <snappy, gzip, lzo>
          | numPartitions
          | resultOutputPath
        """.stripMargin
      )
      sys.exit()
    }

    // 1 接受程序参数
    val Array(logInputPath, compressionCode, numPartitions,resultOutputPath) = args
    // 2 创建sparkconf -> sparkcontext
    val sparkConf = new SparkConf()

    sparkConf.setAppName(s"${this.getClass.getSimpleName}")

    //注意: 在集群中跑的时候, 一定记得把设置本地 master 注释掉, 否则就报如下错误
    /**
      *  diagnostics: Application application_1536038023803_0001 failed 2 times due to AM Container for appattempt_1536038023803_0001_000002 exited with  exitCode: -1000
For more detailed output, check application tracking page:http://cs1:8088/cluster/app/application_1536038023803_0001Then, click on links to logs of each attempt.
Diagnostics: File does not exist: hdfs://mycluster/user/ap/.sparkStaging/application_1536038023803_0001/dmp-1.0-SNAPSHOT.jar
java.io.FileNotFoundException: File does not exist: hdfs://mycluster/user/ap/.sparkStaging/application_1536038023803_0001/dmp-1.0-SNAPSHOT.jar
      */
    sparkConf.setMaster("local[*]")



    // RDD序列化到磁盘  worker 与 worker 之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    // spark2.x 默认是 snappy
    sqlContext.setConf("spark.sql.parquet.compression.codec", compressionCode)

    // 3 读取日志数据 (读取 gbk 数据用下面一种)
    val rawdata: RDD[String] = sc.textFile(logInputPath).repartition(numPartitions.toInt)

//    val rawdata: RDD[String] = sc.hadoopFile(logInputPath, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 1).map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))

    // 4 根据数据需求对数据进行 ETL
    val dataLog: RDD[Log] = rawdata.
      map(line => line.split(",", -1))
      .filter(_.length >= 85)
      .map(Log(_))

    val df: DataFrame = sqlContext.createDataFrame(dataLog)

    df.show()

    df.write.parquet(resultOutputPath)

    sc.stop()
  }

}
