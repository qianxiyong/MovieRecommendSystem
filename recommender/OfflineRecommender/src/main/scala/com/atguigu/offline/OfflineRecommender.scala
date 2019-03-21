package com.atguigu.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

// 创建样例类
// 基于LFM的CF推荐只需要movie和rating数据
case class Movie(mid:Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String,
                 directors: String
                )

case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)

// 创建mongodb配置样例类
case class MongoConfig(uri:String, db:String)

// 定义一个标准推荐对象
case class Recommendation(mid: Int, score: Double)

// 定义一个用户推荐对象
case class UserRecs(uid: Int, recs: Seq[Recommendation])

// 定义一个电影相似度列表对象
case class MovieRecs(mid: Int, recs: Seq[Recommendation])
object OfflineRecommender {
  def consinSim(m1: DoubleMatrix, m2: DoubleMatrix):Double= {
    m2.dot(m1) / m1.norm2() * m2.norm2()
  }


  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"

  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setAppName("OfflineRecommender").setMaster(config("spark.cores"))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    val ratingRDD = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map( rating => (rating.uid,rating.mid,rating.score))
      .cache()

    val userRDD: RDD[Int] = ratingRDD.map(_._1).distinct()

    val movieRDD = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .rdd
      .map(_.mid)
      .cache()

    val trainData = ratingRDD.map(x=>Rating(x._1,x._2,x._3))

    val (rank,iterations,lambda) = (50,5,0.01)
    val model = ALS.train(trainData,rank,iterations,lambda)

    val userMovies = userRDD.cartesian(movieRDD)
    val preRatings: RDD[Rating] = model.predict(userMovies)

    val userRecs = preRatings.filter(_.rating > 0)
      .map(rating => (rating.user,(rating.product,rating.rating)))
      .groupByKey()
      .map{
        case(uid,recs) => UserRecs(uid,recs.toList.sortWith(_._2>_._2).take(10).map(x => Recommendation(x._1,x._2)))
      }
      .toDF()

    userRecs.write
      .option("uri",mongoConfig.uri)
      .option("collection",MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    val movieFeatures = model.productFeatures.map{
      case(mid,features) => (mid,new DoubleMatrix(features))
    }

    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter{
        case(a,b) => a._1 != b._1
      }
      .map{
        case(a,b) => {
          val simScore = this.consinSim(a._2,b._2)
          (a._1,(b._1,simScore))
        }
      }
      .filter(_._2._2>0.6)
      .groupByKey()
      .map{
        case(mid,items) => MovieRecs(mid,items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1,x._2)))
      }.toDF()

    movieRecs.write
      .option("uri",mongoConfig.uri)
      .option("collection",MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    spark.stop
  }
}
