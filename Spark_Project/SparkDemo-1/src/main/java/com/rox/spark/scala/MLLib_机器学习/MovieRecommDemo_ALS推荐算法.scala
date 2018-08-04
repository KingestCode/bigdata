package com.rox.spark.scala.MLLib_机器学习

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

/**
  * ALS推荐算法 实现电影推荐
  */
object MovieRecommDemo_ALS推荐算法 {

  //定义评级样例类
  case class Rating0(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  def main(args: Array[String]): Unit = {

    import org.apache.spark.SparkConf
    val conf = new SparkConf
    conf.setAppName("movieRecomm")
    conf.setMaster("local[4]")

    // 创建SparkSession 对象 spark
    val spark = SparkSession.builder().config(conf).getOrCreate() ;
    import spark.implicits._    // 导入所有隐式转换

    //定义解析评级方法(将数据转为 Rating0对象)
    def parseRating(str: String): Rating0 = {
      val fields = str.split("::")
      assert(fields.size == 4)
      Rating0(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
    }

    //转换成Rating的DF对象
    val ratings = spark.sparkContext.textFile("file:///Users/shixuanji/Documents/Code/Datas/SparkExam/data/mllib/als/sample_movielens_ratings.txt")
    val ratings0 = ratings.map(parseRating)
    // rating0 转为 DataFrame 数据框
    val df = ratings0.toDF()

    //随机切割训练数据，生成两个数组，第一个数组是training,第二个是test
    val Array(training, test) = df.randomSplit(Array(0.99, 0.01))


    //创建ALS推荐算法对象, 并设置参数
    val als = new ALS().setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    //通过als对象对训练数据进行拟合,生成推荐模型
    val model = als.fit(training)

    //使用model对test数据进行变换，实现预测过程
    val predictions = model.transform(test);

    // 搜集打印
    predictions.collect().foreach(println)

  }

}
