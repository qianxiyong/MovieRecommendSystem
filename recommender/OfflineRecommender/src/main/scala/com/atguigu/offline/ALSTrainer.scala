package com.atguigu.offline

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {

  def computeRMSE(model: MatrixFactorizationModel, testRDD: RDD[Rating]):Double = {
    val userMovies = testRDD.map(x => (x.user,x.product))
    val predictRating = model.predict(userMovies)

    val real = testRDD.map(x => ((x.user,x.product),x.rating))
    val predict = predictRating.map(x => ((x.user,x.product),x.rating))

    sqrt(real.join(predict).map{
      case((uid,mid),(real,pre)) =>
        val err = real - pre
        err*err
    }.mean())
  }

  def adjustALSParams(trainRDD: RDD[Rating], testRDD: RDD[Rating]) = {
    val result = for(rank <- Array(20,30,50,100);lambda <- Array(0.001,0.01,0.1,1))
      yield {
        val model = ALS.train(trainRDD,rank,5,lambda)
        val rmse = computeRMSE(model,testRDD)
        (rank,lambda,rmse)
      }
    println("best parameters：" + result.minBy(_._3))
  }

  def main(args: Array[String]): Unit = {
    // 定义用到的配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster("spark.cores").setAppName("OfflineRecommender");
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val MONGODB_RATING_COLLECTION = "Rating"
    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    val ratingRDD = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => Rating(rating.uid,rating.mid,rating.score))
      .cache()

    val splits = ratingRDD.randomSplit(Array(0.8,0.2))
    val trainRDD = splits(0)
    val testRDD = splits(1)

    adjustALSParams(trainRDD,testRDD)

    spark.stop()
  }
}

