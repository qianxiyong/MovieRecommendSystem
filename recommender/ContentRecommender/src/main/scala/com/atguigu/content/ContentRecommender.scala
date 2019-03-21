package com.atguigu.content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix


case class Movie(mid:Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String,
                 directors: String
                )

case class MongoConfig(uri:String,db:String)

// 定义一个标准推荐对象
case class Recommendation(mid: Int, score: Double)

// 定义一个电影相似度列表对象
case class MovieRecs(mid: Int, recs: Seq[Recommendation])
object ContentRecommender {

  val MONGODB_MOVIE_COLLECTION = "Movie"

  val CONTENT_MOVIE_RECS = "COntectMovieRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommeder",
      "mongo.db" -> "recommender"
    )
    val sparkConf = new SparkConf().setAppName("OfflineRecommender").setMaster(config("spark.cores"))

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    val movieTagsDF = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .map(x => (x.mid,x.name,x.genres.map(c=>if(c==' ') ' ' else c)))
      .toDF("mid","name","generes").cache()

    val tokenizer = new Tokenizer().setInputCol("words").setOutputCol("rawFeatures")
    val wordsData: DataFrame = tokenizer.transform(movieTagsDF)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50)
    val featurizedData = hashingTF.transform(wordsData)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    val movieFeatures = rescaledData.map {
      case row => ( row.getAs[Int]("mid"),row.getAs[SparseVector]("features").toArray)
    }
      .rdd
      .map(x => (x._1,new DoubleMatrix(x._2)))
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter{
        case(a,b) => a._1 != b._1
      }
      .map{
        case(a,b) =>
          val simScore = this.consinSim(a._2,b._2)
          (a._1,(b._1,simScore))
      }
      .filter(_._2._2 > 0.6)
      .groupByKey()
      .map{
        case(mid,items) => MovieRecs(mid,items.toList.sortWith(_._2>_._2).map(x => Recommendation(x._1,x._2)))
      }
      .toDF()

    movieRecs.write
      .option("uri",mongoConfig.uri)
      .option("collection",CONTENT_MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix):Double = {
    movie1.dot(movie2) / ( movie1.norm2() * movie2.norm2() )
  }
}
