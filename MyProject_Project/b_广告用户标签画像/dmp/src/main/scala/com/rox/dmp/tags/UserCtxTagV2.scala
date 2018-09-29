package com.rox.dmp.tags

import com.rox.dmp.Beans.Log
import com.rox.dmp.utils.{JedisPools, RptCaculate, Utils}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object UserCtxTagV2 {
  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      println(
        """
          |com.rox.dmp.tags.UserCtxTag
          |参数：
          | inputPath 输入路径
          | appDictPath appId&appName 对照表
          | stopWordPath 过滤词库
          | resultOutputPath 输出路径
        """.stripMargin)
      sys.exit()
    }

    val Array(inputPath, appDictPath, stopWordPath, resultOutputPath) = args


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

    val stopWord: Map[String, Int] = sc.textFile(stopWordPath).map((_, 0)).collect().toMap

    val appDictBC: Broadcast[Map[String, String]] = sc.broadcast(appDict)
    val stopWordBC: Broadcast[Map[String, Int]] = sc.broadcast(stopWord)

    val load: Config = ConfigFactory.load()

    //加载 parquet 文件
    val sQLContext = new SQLContext(sc)
    //过滤掉没有用户标识的数据(如果12个标签都没有值, 就过滤掉)
    val hasUserIdRdd: RDD[Row] = sQLContext.read.parquet(inputPath).where(RptCaculate.hasUserIdCondition).rdd

    // 构建点
    val uv: RDD[(VertexId, List[(String, Int)])] = hasUserIdRdd.mapPartitions(itr => {

      val jedis: Jedis = JedisPools.getConn()
      //创建一个可迭代 list, 用于返回迭代器
      val userIdAndTags = new ListBuffer[(VertexId, List[(String, Int)])]()

      itr.foreach(row => {
        //
        val adTags: Map[String, Int] = Tag4AdSpace.makeTags(row) // 广告的相关标签
        val deviceTags: Map[String, Int] = Tag4Device.makeTags(row, appDictBC.value) //得到 设备,app 标签,  拿到广播变量 appDictBC
        val keywordsTags: Map[String, Int] = Tag4Keywords.makeTags(row, stopWordBC.value) //得到 关键词标签, 拿到广播变量 stopWordBC
        val bTags: Map[String, Int] = Tag4Business.makeTags(row, jedis) //得到商圈信息

        //拿到用户所有设备 id
        val set: mutable.HashMap[String, Int] = Utils.getUserAllIdPlus(row)
        val idAndTags: List[(String, Int)] = (set ++ adTags ++ deviceTags ++ keywordsTags ++ bTags).toList

        var index = 0
        // 同一条数据的所有 tags 应该只出现一次, 当与 set 中第一条用户设备 id 组合时, 拼上 tags, 后面的就拼上 nil
        set.foreach(t => {
          index += 1
          if (index == 1) {
            // 注意: 这里添加的是一个整体元素, 要用() 括起来
            userIdAndTags.append((t._1.hashCode.toLong, idAndTags))
          }
          else {
            userIdAndTags.append((t._1.hashCode.toLong, Nil))
          }
        })
      })
      jedis.close()
      userIdAndTags.toIterator
    })
      // 相同的user, 把所有的属性合并起来
      // (uid, List((k1,1),(k1,2),(k2,1))) => (uid, List((k1,3),(k2,1)))
      .reduceByKey((t1, t2) => {
      (t1 ++ t2).groupBy(_._1).map {
        case (k, tags) => (k, tags.map(_._2).sum)
      }.toList
    })



    //构件边
    val ue: RDD[Edge[Int]] = hasUserIdRdd.flatMap(row => {

      // 获取用户所有特征ID
      val allUserId: mutable.HashMap[String, Int] = Utils.getUserAllIdPlus(row)
      // t=> (String, Int)
      // Edge(map中的第0个元素, 当前遍历到的元素, 0)
      // Edge(0,0,0), Edge(0,1,0),  0,2,0 , .....
      allUserId.map(t => Edge(allUserId.head._1.hashCode.toLong, t._1.hashCode.toLong, 0))
    })


    //构件图
    val graph = Graph(uv,ue)

    graph.connectedComponents().vertices.foreach(println)

    val cc: VertexRDD[VertexId] = graph.connectedComponents().vertices

    /**
      * cc._1是某元素 id, cc._2 是 次元素 id 对应关系图中的最小顶点,
      * cc的数据是这样的:
      *
      * (-895654352,-895654352)
        (-2020910712,-2020910712)
        (-595205686,-595205686)
        (-156308040,-156308040)
        (603235813,-1062100773)
        (1024817156,-421572961)
        (1863097373,-1993285825)
        (749633712,-1072769620)
        (1063844615,964602678)
        (-730050752,-730050752)
        (-2088111103,-2088111103)
      */

    //聚合用户标签数据&用户id数据
    cc.join(uv).map{
      // join 出来, cc._2 就是 commonMinId(最小顶点)
      case (hashUid,(commonMinId,userIdAndTags)) => (commonMinId,userIdAndTags)

        // 根据最小顶点(相当于两个字段的共同好友)来进行 reduce, 保证了有相同的 uuid 的用户的数据可以累加
    }.reduceByKey((list1,list2)=>{
      val listBuffer: List[(String, Int)] = list1 ++ list2
      val grouped: Map[String, List[(String, Int)]] = listBuffer.groupBy(_._1)
      val resulted: Map[String, Int] = grouped.mapValues(lb => lb.map(_._2).sum)
      resulted.toList
    }).foreach(println)
    /**
      * (-394796542,(-394796542,List((APP爱奇艺,1), (CN100018,1), (IMM#2A50B0509D5016AA961E89A865A1BFF4,0), (D00020001,1), (ZP黑龙江省,1), (ZC双鸭山市,1), (D00030004,1), (K网络游戏,1), (K游戏世界,1), (AD#52F0424CD9BCD08B,0), (LN视频暂停悬浮,1), (D00010001,1), (LC09,1))))
(-33274360,(-33274360,List((APP爱奇艺,1), (CN100018,1), (K内地剧场,1), (D00020001,1), (K谍战剧,1), (ZP四川省,1), (D00030004,1), (ZC遂宁市,1), (AD#93DC1A83D94CBEEB,0), (K华语剧场,1), (LN视频暂停悬浮,1), (D00010001,1), (IMM#9F9EA682F0F7E5E316C775C3CFE8C3E8,0), (K悬疑剧,1), (LC09,1))))
(470350404,(13577038,List()))
(-1714111000,(-1714111000,List((APP爱奇艺,1), (IMM#F921589A200742D42CD921764C513E52,0), (ZC保定市,1), (CN100018,1), (D00020001,1), (AD#4F4FE950EDF4E341,0), (LN视频前贴片,1), (K综艺娱乐,1), (D00030004,1), (K脱口秀,1), (ZP河北省,1), (D00010001,1), (LC12,1))))
(-486422041,(-486422041,List((APP爱奇艺,1), (CN100018,1), (AD#8B07EEFC2C7F8B4C,0), (ZC未知,1), (K内地剧场,1), (D00020001,1), (D00030004,1), (K华语剧场,1), (LN视频暂停悬浮,1), (D00010001,1), (IMM#C928F36985C197C8A8AA809354D5D7E1,0), (ZP未知,1), (LC09,1))))
(-591559521,(-591559521,List()))w
      */


    sc.stop()
  }
}
