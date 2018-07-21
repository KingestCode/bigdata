package com.rox.spark.scala

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object RecommDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RecommDemo").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Load and parse the data  加载,解析训练数据
    val data = sc.textFile("file:///Users/shixuanji/Documents/Code/Datas/SparkExam/data/mllib/als/test.data")

    //变换数据成为Rating。
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    // Build the recommendation model using ALS  使用ALS 算法构建推荐模型
    val rank = 10
    val numIterations = 10
    //交替最小二乘法算法构建推荐模型
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    // 取出评分数据的(User,product) (原始训练数据)
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }

    //通过model对(user,product)进行预测
    //    val ug2 = sc.makeRDD(Array((2, 3), (2, 4)))

    // 直接拿出训练数据的 (user, product), 用模型来做匹配, 预测结果的格式是: ((user, product),rate)
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }

    //--------------------------------

    //    //向用户推荐n款商品
    //    val res = model.recommendProducts(5,8);
    //    //将指定的商品推荐给n个用户
    //    val res = model.recommendUsers(3,5)

    //向所有用户推荐3种商品
    val res = model.recommendProductsForUsers(3)
    res.foreach(e => {
      println(e._1 + " ======= ")
      e._2.foreach(println)
    })


    //--------------------------------

    // 打印预测数据
    //    predictions.collect().foreach(println)


    //对训练数据进行map ，((user, product),rate)
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    /**
      * 前面是训练数据, 后面是预测数据
      * 可以看出预测结果非常相近, 因为预测的数据就是训练的数据
      */
    ratesAndPreds.collect().foreach(println)

    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    println("Mean Squared Error = " + MSE)

    // Save and load model
    model.save(sc, "file:///Users/shixuanji/Documents/Code/Datas/SparkExam/mydata/recomm_model1")
    //    val sameModel = MatrixFactorizationModel.load(sc, "file:///Users/shixuanji/Documents/Code/Datas/SparkExam/mydata/recomm_model1")
  }

}
