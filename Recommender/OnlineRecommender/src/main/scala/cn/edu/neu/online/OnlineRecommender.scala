package cn.edu.neu.online

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson
import org.mongodb.scala.MongoClient

import scala.concurrent.{Await, Future}
import redis.clients.jedis.Jedis
import org.mongodb.scala.model.Filters._

object ConnHelper extends Serializable{
  //lazy变量，使用的时候才会被初始化
//  lazy val jedis = new Jedis("localhost")
//  lazy val mongoClient = MongoClient("mongodb://localhost:27017/erecommender")
   val jedis = new Jedis("localhost")
   val mongoClient = MongoClient("mongodb://localhost:27017/erecommender")

}

case class MongoConfig(uri:String, db:String)
// 标准推荐对象，productId,score
case class Recommendation(productId: Int, score:Double)
// 用户推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])
// 商品相似度（商品推荐）
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

object OnlineRecommender {
  //从redis中选取评分的最大长度
  val MAX_USER_RATINGS_NUM = 20
  //候选列表的最大长度，相似的商品数
  val MAX_SIM_PRODUCTS_NUM = 20
  val STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  //相似度列表
  val PRODUCT_RECS_COLLECTION = "ProductRecs"

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Software\\playfootball-winutils-master\\winutils\\hadoop-2.7.1")
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/erecommender",
      "mongo.db" -> "erecommender",
      "kafka.topic" -> "recommen2"
    )
    //创建一个SparkConf配置
    val sparkConf = new SparkConf().setAppName("StreamingRecommender").setMaster(config("spark.cores"))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc,Seconds(2))

    // 测试代码
//    val lines = ssc.socketTextStream("localhost", 6789)
//    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
//    result.print()


    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._
    // 广播商品相似度矩阵
    val simProductsMatrix: collection.Map[Int, Map[Int, Double]] = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",PRODUCT_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRecs]
      .rdd
      //为了后续查询相似度方便，把数据转换成map形式
      .map{item =>
        (item.productId,item.recs.map(x=> (x.productId,x.score)).toMap)
      }.collectAsMap()

//    println("simProductsMatrix:")
//    simProductsMatrix.foreach(t =>{
//      println((t._1,t._2))
//    })

    //定义广播变量
    val simProductsMatrixBroadCast: Broadcast[collection.Map[Int, Map[Int, Double]]] = sc.broadcast(simProductsMatrix)

    val spmb: collection.Map[Int, Map[Int, Double]] = simProductsMatrixBroadCast.value
    println("simProductsMatrixBroadCast:")
    spmb.foreach(t=>{
      println(((t._1),t._2))
    })

    //创建到Kafka的连接
    val kafkaParam = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    val kafkaStream = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(config("kafka.topic")),kafkaParam))

    // 对kafkaStream进行处理，产生评分流：UID|MID|SCORE|TIMESTAMP
    val ratingStream: DStream[(Int, Int, Double, Int)] = kafkaStream.map{ msg=>                                                //case？
       var attr = msg.value().split("\\|")
      (attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)

    }
//    // 核心实时推荐算法
     ratingStream.foreachRDD{
       rdd => rdd.foreach{
         case (userId: Int, productId: Int, score: Double, timestamp: Int) =>{

           println("rating data coming!>>>>>>>>>>>>>>>>")

           print("新的评分From Kafka：")
           println(userId, productId,score, timestamp)

           // 1. 从redis中获取当前最近的M次商品评分，保存成一个数组Array[(productId, score)]
           val userRecentlyRatings: Array[(Int, Double)] = getUserRecentlyRating(MAX_USER_RATINGS_NUM,userId,ConnHelper.jedis)

           userRecentlyRatings.foreach(item =>print("userRecentlyRatings:"+(item._1,item._2)))
           println()

           // 2. 获取商品P最相似的K个商品,从相似度矩阵中获取当前商品最相似的商品列表，作为备选列表，保存为一个数组Array[productId]
           val candidateProducts: Array[Int] = getTopSimProducts(MAX_SIM_PRODUCTS_NUM,productId,userId,simProductsMatrixBroadCast.value)

           candidateProducts.foreach(item => print("candidateProducts:"+item))

           //计算待选商品的推荐优先级，得到当前用户的实时推荐列表, 保存为Array[(productId, score)]
           val streamRecs: Array[(Int, Double)] = computeProductScores(simProductsMatrixBroadCast.value,userRecentlyRatings,candidateProducts)
           print("streamRecs")
           streamRecs.foreach(item =>print(item))

           //将数据保存到MongoDB
           saveRecsToMongoDB(userId,streamRecs)

           }
       }

//        1. 从redis中获取当前最近的M次商品评分，保存成一个数组Array[(productId, score)]
//        val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM,userId,ConnHelper.jedis)
//        2. 获取商品P最相似的K个商品,从相似度矩阵中获取当前商品最相似的商品列表，作为备选列表，保存为一个数组Array[productId]
//        val candidateProducts = getTopSimProducts(MAX_SIM_PRODUCTS_NUM,productId,userId,simProductsMatrixBroadCast.value)
//        计算待选商品的推荐优先级，得到当前用户的实时推荐列表, 保存为Array[(productId, score)]
//        val streamRecs = computeProductScores(simProductsMatrixBroadCast.value,userRecentlyRatings,candidateProducts)
//        将数据保存到MongoDB
//        saveRecsToMongoDB(userId,streamRecs)

     }

    //启动Streaming程序
    ssc.start()
    println("streaming started")
    ssc.awaitTermination()
  }
   import scala.collection.JavaConverters._
//  import java.util._
  def getUserRecentlyRating(num: Int, userId: Int, jedis: Jedis): Array[(Int, Double)] = {
    //从redis中用户的评分队列里获取评分数据，list的键名为uid: USERID,值格式是PRODUCTID：SCORE
    jedis.lrange("userId:" + userId.toString, 0, num).asScala.map{
      item =>
        val attr = item.split("\\:")
        (attr(0).trim.toInt, attr(1).trim.toDouble)

    }.toArray
  }
  def getTopSimProducts(num: Int,
                        productId: Int,
                        userId: Int,
                        simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                       (implicit mongoConfig: MongoConfig): Array[Int]={
    //从广播变量相似度矩阵中拿到当前商品的相似度列表
    val allSimProducts = simProducts.get(productId).get.toArray

    allSimProducts.foreach(item => print("allSimProductsBeforeFilter:"+(item._1,item._2)))
    println()

    //获取用户已经观看过得商品
    val ratingCollection = ConnHelper.mongoClient.getDatabase(mongoConfig.db).getCollection(MONGODB_RATING_COLLECTION)
    import scala.concurrent.duration._

//    val Existratings = ratingCollection.find(equal("userId",userId)).projection(fields(include("productId")))
//     val ee = Existratings.subscribe ( new Observer[Document] {
//      override def onNext(result: Document): Unit = println(result.toJson())
//      override def onError(e: Throwable): Unit = println("Failed" + e.getMessage)
//      override def onComplete(): Unit = println("Completed")
//    })

    val e: Future[Seq[Int]] = ratingCollection.find(equal("userId",userId)).map{
      item => item.get("productId").get.asInt32().getValue
    }.toFuture()
   // val ratingExist =Await.result(e, 2.seconds).toArray
    val ratingExist =Await.result(e, Duration.Inf).toArray
  //  for (elem <- ratingExist) {print("lala:"+ elem)}

    println("ratingExist_productId:")
    ratingExist.foreach(x=>print(x + ","))
    println()

    //过滤掉已经评分过得商品，并排序输出
    allSimProducts.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2).take(num).map(x => x._1)
    //获得用户已经评分过的商品，过滤掉，排序输出
  }

  //计算每个备选商品的推荐得分
  def computeProductScores(simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]],
                           userRecentlyRatings: Array[(Int, Double)],
                           candidateProducts: Array[Int]): Array[(Int, Double)] = {
    // 定义一个长度可变的数组ArrayBuffer, 用于保存每一个备选商品的基础得分(productId, score)
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    //  定义两个Map, 用于保存每个商品的高分和低分的计数器, productId -> count
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    //遍历每个备选商品，计算和已评分商品的相似度
    for (candidateProcduct <- candidateProducts; userRecentlyRating <- userRecentlyRatings){
      //从相似度矩阵中获取当前备选商品和当前已评分商品间的相似度
      val simScore = getProductsSimScore(candidateProcduct, userRecentlyRating._1, simProducts)
      if ( simScore >= 0.4 ){
        //按照公式进行计算,得到基础评分
        scores += ((candidateProcduct, simScore * userRecentlyRating._2))
        if(userRecentlyRating._2 > 3){
          increMap(candidateProcduct) = increMap.getOrElse(candidateProcduct, 0) + 1
        } else {
          decreMap(candidateProcduct) = decreMap.getOrElse(candidateProcduct, 0) + 1
        }
      }
    }
    //根据公式计算所有的推荐优先级，首先以productId做groupBy
    scores.groupBy(_._1).map{
      case (productId, scorelist) =>
        (productId,scorelist.map(_._2).sum/scorelist.length + log(increMap.getOrElse(productId, 1)) - log(decreMap.getOrElse(productId, 1)))
    }
      //返回推荐列表，按照的得分排序
      .toArray
      .sortWith(_._2>_._2) //按照降序排列
  }
  def getProductsSimScore(product1: Int, product2: Int,
                          simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]):Double = {
    simProducts.get(product1) match {
      case Some(sims) => sims.get(product2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }
  //自定义log函数, 以N为底
  def log(m: Int): Double = {
    val N = 10
    math.log(m)/math.log(N)
  }
  //写入mmongodb
  import org.mongodb.scala.bson.collection.immutable.Document
//  import org.mongodb.scala.bson.Document
  def saveRecsToMongoDB(userId: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit ={
    val streamRecsCollection = ConnHelper.mongoClient.getDatabase(mongoConfig.db).getCollection(STREAM_RECS_COLLECTION)
    //按照userId查询更新
    streamRecsCollection.findOneAndDelete(equal("userId",userId))
    val recs: Array[Document] = streamRecs.map(item=>Document("productId"->item._1,"score"->item._2))
    val doc:Document = Document("userId" -> userId,"recs"->recs) // bugs
    streamRecsCollection.insertOne(doc)
  }
}
