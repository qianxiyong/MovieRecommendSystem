package com.atguigu.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

// 创建样例类
// 统计推荐只需要movie和rating数据
case class Movie(mid:Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String,
                 directors: String
                )

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

// 创建mongodb配置样例类
case class MongoConfig(uri:String, db:String)

case class Recommendation(mid:Int,score:Double)

case class GenresRecommendation(genres: String,recs: Seq[Recommendation])

object StatisticsRecommender {

  val MONGODB_MOVIE_COLLECTION = "Movie";
  val MONGODB_RATING_COLLECTION = "Rating"

  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri",mongoConfig.uri)
      .option("collection",collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    val ratingDF  = spark.read.option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    ratingDF.createOrReplaceTempView("ratings")

    val rateMoreMoviesDF: DataFrame = spark.sql("select mid,count(mid) as count from ratings group by mid order by count desc")
    storeDFInMongoDB(rateMoreMoviesDF,RATE_MORE_MOVIES)

    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changeDate",(x:Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)
    val ratingOfYearMonth = spark.sql("select mid,score,changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    val rateMoreRecentlyMoviesDF: DataFrame = spark.sql("select yearmonth,mid,count(mid) as count from ratingOfMonth group by yearmonth,mid order by yearmonth,count desc")
    storeDFInMongoDB(rateMoreRecentlyMoviesDF,RATE_MORE_RECENTLY_MOVIES)

    val averageMoviesDF: DataFrame = spark.sql("select mid,avg(score) as avg from ratings group by mid")
    storeDFInMongoDB(averageMoviesDF,AVERAGE_MOVIES)

    val genres = List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")

    val movieWithScore: DataFrame = movieDF.join(averageMoviesDF,"mid")

    val genresRDD = spark.sparkContext.makeRDD(genres)

    val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd)
      .filter{
        case(genres,row) => row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase())
      }
      .map{
        case(genres,row) => {
          (genres,(row.getAs[Int]("mid"),row.getAs[Double]("avg")))
        }
      }
      .groupByKey()
      .map{
        case (genres,items) => {
          GenresRecommendation(genres,items.toList.sortWith(_._2>_._2).take(10).map(item => Recommendation(item._1,item._2)))
        }
      }
      .toDF()
    storeDFInMongoDB(genresTopMoviesDF,GENRES_TOP_MOVIES)
    spark.stop
  }
}
