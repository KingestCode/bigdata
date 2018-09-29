package com.rox.dmp.report

import com.rox.dmp.Beans.Log
import com.rox.dmp.utils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 需求:
  *
  *
  *
  * 省市/城市         原   始  请     有效请求         广告请求          参与竞价         竞  价  成 功     竞价成功          展示      点击      点击       广  告  成     广  告  消

              求                                       数            数             率             量       量       率        本           费

-A 省                                                  1000         600           30.56%        800     800     2%       15.87       15.87

    B 市                                               200          100           30.56%        178     178     2%       15.87       15.87

    C 市                                               100          50            30.56%        78      78      2%       15.87       15.87

    D 市                                               400          200           30.56%        324     324     2%       15.87       15.87

    F 市                                               300          250           30.56%        167     167     2%       15.87       15.87

+B  省                                                 1000         600           23.23%        700     323     4%       98.21       50.81
  */

object AreaAnalyseRpt_rdd {

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

    //#################################
    //读取 原始 数据
    //#################################
    sc.textFile(logInputPath)
      .map(line => line.split(",", -1))
      .filter(t => t.length >= 85)
      .map(arr => {
        val log = Log(arr)
        val reqList: List[Double] = RptUtils.calculateReq(log.requestmode, log.processnode)
        val rtbList: List[Double] = RptUtils.calculateRtb(log.iseffective, log.isbilling, log.isbid, log.adorderid, log.iswin, log.winprice, log.adpayment)
        val showClickList: List[Double] = RptUtils.calculateShowClick(log.requestmode, log.iseffective)
        ((log.provincename,log.cityname),reqList ++ reqList ++ rtbList)
      })
      .reduceByKey((l1,l2) => {
        l1.zip(l2).map(t => t._1 + t._2)
      }).map(t => t._1._1 + ","+ t._1._2 + "," + t._2.mkString(","))
      .saveAsTextFile(resultOutputPath)











//#################################
    //读取 parquet 数据
//#################################

    /**
      * 使用 rdd 处理数据
      * 最后想得到的数据 ((province, city), List(x,x,x,x,...))
      * 这里最好先转为 rdd 操作, 否则会报这个错误, 参考 2.0后会有此错
      * https://blog.csdn.net/sparkexpert/article/details/52871000
      * Error:(74, 20) Unable to find encoder for type stored in a Dataset.  Primitive types (Int, String, etc) and Product types (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in future releases.
    parquetData.map(row => {
      */
/*
    val sQLContext = new SQLContext(sc)
    //加载 parquet 数据
    val parquetData: DataFrame = sQLContext.read.parquet(logInputPath)

    parquetData.rdd.map(row => {
      //
      val reqMode = row.getAs[Int]("requestmode")
      val prcNode = row.getAs[Int]("processnode")
      // 参与竞价, 竞价成功  List(参与竞价，竞价成功, 消费, 成本)
      val effTive = row.getAs[Int]("iseffective")
      val bill = row.getAs[Int]("isbilling")
      val bid = row.getAs[Int]("isbid")
      val orderId = row.getAs[Int]("adorderid")
      val win = row.getAs[Int]("iswin")
      val winPrice = row.getAs[Double]("winprice")
      val adPayMent = row.getAs[Double]("adpayment")
      val province = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")

      val reqList: List[Double] = RptUtils.calculateReq(reqMode, prcNode)
      val rtbList: List[Double] = RptUtils.calculateRtb(effTive, bill, bid, orderId, win, winPrice, adPayMent)
      val showClickList: List[Double] = RptUtils.calculateShowClick(reqMode, effTive)

      // 返回元祖
      ((province, city), reqList ++ rtbList ++ showClickList)
    })
      .reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    })
      // 将((string,string), List[Double])拆散
      .map(t => {
      t._1._1 + "," + t._1._2 + t._2.mkString(",")
    }).saveAsTextFile(resultOutputPath)
*/

  }

}
