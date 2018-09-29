package com.rox.dmp.report

import com.rox.dmp.Beans.Log
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用 rdd 处理
  */
object ProCityRptV3_rdd {

  def main(args: Array[String]): Unit = {
    // 0 校验参数个数
    if (args.length != 2) {
      println(
        """
          |com.rox.dmp.tools.ProCityRptV3_rdd
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

    // 如果目录已经存在, 就删除目录
    val configuration: Configuration = sc.hadoopConfiguration
    val fs: FileSystem = FileSystem.get(configuration)
    val resultPath = new Path(resultOutputPath)
    if (fs.exists(resultPath)) {
      fs.delete(resultPath)
    }

    // 使用 rdd 统计省市的数量
    sc.textFile(logInputPath)
      .map(_.split(",", -1))            // 使用","分割
      .filter(_.length >= 6)            // 过滤掉字段数少于6的
      // ((湖北省,武汉市), 1)
      .map(arr => ((arr(0), arr(1)), 1)) //map 变换为元祖格式, 拼1
      .reduceByKey(_ + _)                //value 相加
      // 湖北省,武汉市,1
      .map(t => t._1._1 + "," + t._1._2 + "," + t._2)  //转换格式
      .coalesce(1)                                    //合并为1个分区
      .saveAsTextFile(resultOutputPath)               //保存文件

    sc.stop()
  }

}
