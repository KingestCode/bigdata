package com.rox.dmp.tags

import com.rox.dmp.Beans.Log
import com.rox.dmp.utils.{JedisPools, RptCaculate}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.http.{HttpHost, HttpRequest, HttpResponse}
import org.apache.http.client.{HttpClient, ResponseHandler}
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.conn.ClientConnectionManager
import org.apache.http.params.HttpParams
import org.apache.http.protocol.HttpContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object UserCtxTag {

  def main(args: Array[String]): Unit = {

    if (args.length != 5) {
      println(
        """
          |com.rox.dmp.tags.UserCtxTag
          |参数：
          | inputPath 输入路径
          | appDictPath appId&appName 对照表
          | stopWordPath 过滤词库
          | day 日期
          | resultOutputPath 输出路径
        """.stripMargin)
      sys.exit()
    }

    val Array(inputPath, appDictPath, stopWordPath, day, resultOutputPath) = args


    // 2 创建sparkconf->sparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[Log]))

    val sc = new SparkContext(sparkConf)

    //加载并广播,  appDict & stopWord
    val appDict: Map[String, String] = sc.textFile(appDictPath).map(line => {
      line.split("\t", -1)
    }).filter(_.length >= 5)
      .filter(arr => {
        arr(4).startsWith("com.")
      }).map(arr => (arr(4), arr(1)))
      .collect().toMap

    val stopWord: Map[String, Int] = sc.textFile(stopWordPath).map((_,0)).collect().toMap

    val appDictBC: Broadcast[Map[String, String]] = sc.broadcast(appDict)
    val stopWordBC: Broadcast[Map[String, Int]] = sc.broadcast(stopWord)

    val load: Config = ConfigFactory.load()
    val hbTableName: String = load.getString("hbase.table.name")
    val cfName: String = load.getString("hbase.table.columeFamily")
    /**
      * hbase 相关
      */
    //判断 hbase 中的表是否存在, 如果不存在, 则创建
//    val configuration: Configuration = sc.hadoopConfiguration
//    // 设置 hbase 的 zk 地址
//    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.zookeeper.host"))
//    // 通过连接工厂创建连接
//    val hbConn: Connection = ConnectionFactory.createConnection(configuration)
//    // 通过连接拿到 admin
//    val hbAdmin: Admin = hbConn.getAdmin
//
//    //如果hbase表不存在
//    if (!hbAdmin.tableExists(TableName.valueOf(hbTableName))){
//      println(s"$hbTableName 不存在...")
//      println(s"正在创建 $hbTableName ...")
//
//      //创建表描述器
//      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbTableName))
//      //创建列族描述器
//      val hColumnDescriptor = new HColumnDescriptor(cfName)
//      //创建列族
//      tableDescriptor.addFamily(hColumnDescriptor)
//      //创建表
//      hbAdmin.createTable(tableDescriptor)
//
//      //释放连接
//      hbAdmin.close()
//      hbConn.close()
//    }
//
//    //指定 key 的输出类型
//    val jobConf = new JobConf(configuration)
//    jobConf.setOutputFormat(classOf[TableOutputFormat])
//    //指定表的名称
//    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbTableName)


    //加载 parquet 文件
    val sQLContext = new SQLContext(sc)
    //过滤掉没有用户标识的数据(如果12个标签都没有值, 就过滤掉)
    sQLContext.read.parquet(inputPath).where(RptCaculate.hasUserIdCondition).rdd
      .mapPartitions(itr => {

        val jedis: Jedis = JedisPools.getConn()
        //创建一个可迭代 list, 用于返回迭代器
        val lsb = new collection.mutable.ListBuffer[(String, List[(String, Int)])]()

        itr.foreach(row => {
          //
          val adTags: Map[String, Int] = Tag4AdSpace.makeTags(row)
          //得到 设备,app 标签,  拿到广播变量 appDictBC
          val deviceTags: Map[String, Int] = Tag4Device.makeTags(row, appDictBC.value)
          //得到 关键词标签, 拿到广播变量 stopWordBC
          val keysTags: Map[String, Int] = Tag4Keywords.makeTags(row,stopWordBC.value)
          //得到用户唯一标识
          val userIds: ListBuffer[String] = RptCaculate.getUserId(row)
          //得到商圈信息
          val business: Map[String, Int] = Tag4Business.makeTags(row, jedis)

          //拿到除第一个之外的其它的用户id
          val otherUserIds: Map[String, Int] = userIds.slice(1,userIds.length-1).map(str => (str,0)).toMap

          lsb.append((userIds(0), (adTags ++ deviceTags ++ keysTags ++ business).toList))
        })
        jedis.close()
        lsb.toIterator
      })         // 这里得到一个 rdd

      // 相同的user, 把所有的属性合并起来
      // (uid, List((k1,1),(k1,2),(k2,1))) => (uid, List((k1,3),(k2,1)))
      .reduceByKey((t1,t2) => {

      // 方式1 偏函数
      (t1 ++ t2).groupBy(_._1).map{
        case (k, tags) => (k, tags.map(_._2).sum)
      }.toList

      /**方式2
        * 左折叠
        * 左边的_是传入的0
          scala> List(1,7,2,9).foldLeft(0)(_-_)
          res87: Int = -19
              (0 - 1) - 7 - 2 - 9            //-19
        */
//      (t1 ++ t2).groupBy(_._1).mapValues(t => t.foldLeft(0)(_ + _._2)).toList
    })
//    //存到 hbase
//      .map{
//      case (userId, userTags) => {
//
//        val put = new Put(Bytes.toBytes(userId))
//
//        // 接下来把这一堆标签作为一个整体值存到一个列中
//        // 转为字符串 list 元素 => list String => list 转为字符串, 每个元素之间用,隔开
//        val tags: String = userTags.map(t => t._1 +  ":" + t._2).mkString(",")
//
//        put.addColumn(Bytes.toBytes(cfName),Bytes.toBytes(s"day$day"),Bytes.toBytes(tags))
//
//        (new ImmutableBytesWritable(), put)  //ImmutableBytesWritable -> rowkey
//      }
//        //存到 hbase 中
//    }.saveAsHadoopDataset(jobConf)
    //存到本地
      .saveAsTextFile(resultOutputPath)



    sc.stop()
  }
}
