package com.atguigu.streaming

import java.util

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.jackson.map.deser.std.StringDeserializer
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
object ConnHelper extends Serializable {

  lazy val jedis = new Jedis("localhost")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))
}

case class MongoConfig(uri:String,db:String)

case class Recommendation(mid:Int,score:Double)

case class UserRecs(uid:Int,recs:Seq[Recommendation])

case class MovieRecs(mid:Int,recs:Seq[Recommendation])
class StreamingRecommender {

  val MAX_USER_RATING_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"


  def getTopSimMovies(n: Int, mid:Int, uid: Int, simMovies: collection.Map[Int, Map[Int, Double]])(implicit mongoConfig: MongoConfig):Array[Int] = {
    val allSimMovies = simMovies(mid).toArray
    val ratingExsit: Array[Int] = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).find(MongoDBObject("uid" -> uid)).toArray.map {
      item => item.get("mid").toString.toInt
    }

    allSimMovies.filter(x => !ratingExsit.contains(x._1)).sortWith(_._2 > _._2).take(n).map(x=>x._1)
  }

  def getMovieSimScore(mid1: Int, mid2: Int, simMovies: collection.Map[Int, Map[Int, Double]]):Double = {
    simMovies.get(mid1) match {
      case Some(sim) => sim.get(mid2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  def log(m: Int) = {
    math.log(m) / math.log(2)
  }

  def computeMovieScores(simMovies: collection.Map[Int, Map[Int, Double]], userRecentlyRatings: Array[(Int, Double)], candidateMovies: Array[Int]):Array[(Int, Double)] = {
    val scores = scala.collection.mutable.ArrayBuffer[(Int,Double)]()
    val increMap = scala.collection.mutable.HashMap[Int,Int]()
    val decreMap = scala.collection.mutable.HashMap[Int,Int]()

    for(candidateMovie <- candidateMovies;userRecentlyRating <- userRecentlyRatings) {
      val simScore = getMovieSimScore(candidateMovie,userRecentlyRating._1,simMovies)
      if(simScore > 0.6) {
        scores += ((candidateMovie,simScore * userRecentlyRating._2))

        if(userRecentlyRating._2 > 3) {
          increMap(candidateMovie) = increMap.getOrDefault(candidateMovie,0) + 1
        }else{
          decreMap(candidateMovie) = decreMap.getOrDefault(candidateMovie,0) + 1
        }
      }
    }

    scores.groupBy(_._1).map{
      case(mid,scoreLists) => (mid,scoreLists.map(_._2).sum/scoreLists.length+log(increMap.getOrDefault(mid,1))-log(decreMap.getOrDefault(mid,1))))
    }
  }

  def saveRecsToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    streamRecsCollection.findAndRemove(MongoDBObject("uid"->uid))
    streamRecsCollection.insert(MongoDBObject("uid"->uid,"recs"->streamRecs.map(x=>MongoDBObject("mid"->x._1,"score"->x._2))))
  }

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost；27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc,Seconds(2))

    import spark.implicits._
    implicit val mongoconfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    val simMoviesMatrix = spark.read
      .option("uri",config("mongo.uri"))
      .option("collection",MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map{
        item => (item.mid,item.recs.map(x => (x.mid,x.score)).toMap)
      }
      .collectAsMap()

    val simMoviesMatrixBroadcast = sc.broadcast(simMoviesMatrix)

    val kafkaPara = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara)
    )

    val ratingStream: DStream[(Int, Int, Double, Int)] = kafkaStream.map {
      case msg =>
        val attr = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }
    ratingStream.foreachRDD{
      rdd => rdd.foreach{
        case(uid,mid,score,timestamp) => {
          println("rating data coming!>>>>>"+(uid,mid,score,timestamp))

          val userRecentlyRatings = getUserRecentlyRatings(MAX_USER_RATING_NUM,uid,ConnHelper.jedis)

          val simMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM,mid,uid,simMoviesMatrixBroadcast.value)

          val streamRecs = computeMovieScores(simMoviesMatrixBroadcast.value,userRecentlyRatings,simMovies)

          saveRecsToMongoDB(uid,streamRecs)
        }
      }
    }

    ssc.start()
    println("streaming started!")
    ssc.awaitTermination()
  }

  def getUserRecentlyRatings(k: Int, uid: Int, jedis: Jedis):Array[(Int,Double)] = {
    jedis.lrange("uid:"+uid,0,k-1).map{
      item => val attr = item.split("\\:")
        (attr(0).trim.toInt,attr(1).trim.toInt)
    }.toArray
  }

}
