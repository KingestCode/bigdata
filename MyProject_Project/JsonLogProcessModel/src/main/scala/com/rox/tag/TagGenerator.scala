package com.rox.tag

import org.apache.spark.{SparkConf, SparkContext}

object TagGenerator {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("TagGenerator")
    val sc = new SparkContext(conf)

    // read data
    val poi_tags = sc.textFile("/Users/shixuanji/Documents/Code/Datas/tagGen/temptags.txt")

    // split 2 array && length of array if 2, if not, discard
    val poi_taglist =
      poi_tags.map(e => e.split("\t"))
        .filter(e => e.length == 2)
        .map(e => e(0) -> ReviewTags.extractTags(e(1)))

        /**
          * (79197522,价格实惠,干净卫生,体验舒服,技师专业,服务热情)
          * (85766086,环境优雅,价格实惠,菜品不错)
          * (85766086,味道赞,服务热情,性价比高,体验好,干净卫生)
          */
        .filter(e => e._2.length > 0)

        /**
          * (79197522,[味道赞,环境优雅])
          */
        .map(e => e._1 -> e._2.split(","))

        /**
          * flatMapValues the v
          * (75144086,技师专业)
          * (75144086,效果赞)
          * (75144086,体验好)
          */
        .flatMapValues(e => e)

        /**
          * or map(e=> (e._1,e._2)->1)
          * ((70611801,环境优雅),1)
          * ((70611801,回头客),1)
          * ((70611801,分量足),1)
          */
        .map((_, 1))

        /**
          * ((82016443,环境优雅),2)
          * ((77287793,音响效果好),26)
          */
        .reduceByKey(_ + _)

        /**
          * tuple._2 turn to list to aggregation a shop's all tips
          * (82016443,List(环境优雅,2))
          * (77287793,List(音响效果好,26))
          */
        .map(e => e._1._1 -> List(e._1._2 -> e._2))

        /**
          * 如果2边都是 list 集合, 必须用 ++:或者 ::: 或者 ++
          * 根据 key, 化简 list
          * (83644298,List((体验好,1), (性价比高,1), (服务热情,1), (价格实惠,1), (味道赞,1)))
          * (71039150,List((团建,1), (价格实惠,1), (朋友聚会,1), (环境优雅,1), (体验好,1)))
          */
        .reduceByKey(_ ::: _)

        /**
          * Tuble2
          * (83644298, 味道赞:1,价格实惠:1,服务热情:1,性价比高:1,体验好:1)
          * (82317795, 味道差:1)
          */
        .map(e =>
        // 把每个 list 中的元祖, 用, 连接
        e._1 -> e._2.sortBy(_._2).reverse.take(10).map(e => e._1 + ":" + e._2.toString).mkString(","))

        // 以制表符分割元祖中2个元素
        /**
          * 以制表符分割元祖中2个元素
          * 83644298	味道赞:1,价格实惠:1,服务热情:1,性价比高:1,体验好:1
          * 82317795	味道差:1
          * 77705462	服务热情:3,环境优雅:2,价格实惠:2,羊肉:2,干净卫生:1,肉类好:1,回头客:1,羊蝎子:1,味道赞:1
          * 85766086	干净卫生:2,味道赞:2,服务热情:2,价格实惠:2,菜品不错:1,环境优雅:1,体验好:1,性价比高:1,服务差:1,上菜慢:1
          */
          .map(e => e._1 + "\t" + e._2)

      .saveAsTextFile("/Users/shixuanji/Documents/Code/Datas/tagGen/res")

//        .collect()
//        .foreach(println)

    //        .foreach(e=>{
    //        println(e._1)
    //        for (ee <- e._2) {
    //          println(ee)
    //        }
    //      })
  }

}
