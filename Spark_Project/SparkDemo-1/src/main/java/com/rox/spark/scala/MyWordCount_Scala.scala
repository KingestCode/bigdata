package com.rox.spark.scala

import org.apache.spark.{SparkConf, SparkContext}


/**
  * countByKey, 统计 key 的个数
  * reduceByKey, 根据 key 计算相同 key 后的 v
  */
object MyWordCount_Scala {

  def main(args: Array[String]): Unit = {

    //创建 spark 配置对象
    val conf = new SparkConf()

    conf.setAppName("MyWordCount_Scala")

    //设置 master 属性
    conf.setMaster("local")

    // 通过 conf 创建 sc (注意, 这里一定要传入 conf, 否则就会报下面这个错误)
    // A master URL must be set in your configuration
    val sc = new SparkContext(conf)

    // 加载本地文件
    val rdd1 = sc.textFile("/Users/shixuanji/Documents/IDEs/iTerm2/scala/a.txt");
    //    val rdd1 = sc.textFile(args(0))

    val sumWordCount = rdd1.map(_.split(" ").length).reduce(_ + _)
    println("---------这里也是总单词个数" + sumWordCount + "---------")


    // 压扁
    val rdd2 = rdd1.flatMap(line => {
      println(line);
      line.split(" ")
    }).filter(_.contains("wor"))

    // map 映射成 word => (word, 1)
    val rdd3 = rdd2.map((_, 1))

    // countByKey, 统计 key 的个数
    // reduceByKey, 根据 key 计算相同 key, 后的 v 的和
    rdd3.countByKey().foreach(println)

    // reduceByKey, 按照相同的单词, 把次数加起来 hello.1, word 2, .....,但是是 rdd 格式的
    // 这里得到的是多个每个 , 可以把rdd4缓存起来, 如果还需要用可以直接取出用
    // 后面还可以加上分区数
    val rdd4 = rdd3.reduceByKey(_ + _, 2)


    // 这里可以存储到文件中
    rdd4.saveAsTextFile("/Users/shixuanji/Documents/IDEs/iTerm2/scala/savetest")
    //    rdd4.saveAsObjectFile()
    //    rdd4.saveAsSequenceFile()
    //     .....

    // 这里把所有的数字加起来, 就可以统计单词的个数了
    val rdd5 = rdd4.map(_._2); // 这个是拿到前 rdd4 元祖的第二个元素 1,3,4,..

    // 对 rdd5再做 reduce 聚合, 就可以得到总单词数(2个 Int 返回一个 Int)
    val count = rdd5.reduce(_ + _) //纵向捏合


    println("总单词数: " + count) // 打印总单词数 (这里是 filter 掉带有 wor 后的单词各个数)


    // collect
    val r = rdd4.collect()

    // 遍历输出
    r.foreach(println)
  }
}
