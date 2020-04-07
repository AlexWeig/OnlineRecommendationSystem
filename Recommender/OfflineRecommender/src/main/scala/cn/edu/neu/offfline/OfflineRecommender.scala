package cn.edu.neu.offfline

import breeze.linalg.rank
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix


case class ProductRating(userId: Int, productId: Int, score: Double, timestamp: Int)
case class MongoConfig(uri:String, db:String)

// 标准推荐对象，productId,score
case class Recommendation(productId: Int, score:Double)

// 用户推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])

// 商品相似度（商品推荐）
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

object OfflineRecommender {
  // 定义常量
  val MONGODB_RATING_COLLECTION = "Rating"

  // 推荐表的名称
  val USER_RECS = "UserRecs"
  val PRODUCT_RECS = "ProductRecs"

  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\Software\\playfootball-winutils-master\\winutils\\hadoop-2.7.1")
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/erecommender",
      "mongo.db" -> "erecommender"
    )
    //创建SparkConf配置
    val sparkConf = new SparkConf().setAppName("OfflineRecommender").setMaster(config("spark.cores"))
    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //加入隐式转换
    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    //加载数据
    val ratingRDD = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(rating=> (rating.userId, rating.productId, rating.score)).cache()
    //用户的数据集 RDD[Int]
    val userRDD = ratingRDD.map(_._1).distinct()
    val productRDD = ratingRDD.map(_._2).distinct()

    // TODO:核心计算过程
    // 1. 训练隐语义模型
    val trainData = ratingRDD.map(x => Rating(x._1,x._2,x._3.toFloat))
    val (rank,iterations,lambda) = (50, 5, 0.01)
    // 调用ALS算法训练隐语义模型
    val model = ALS.train(trainData,rank,iterations,lambda)


    // 2.获得预测评分矩阵，得到用户的推荐列表

    //userRDD和productRDD作笛卡尔积
    val userProducts = userRDD.cartesian(productRDD)
    val preRatings = model.predict(userProducts)

    //从预测评分矩阵，得到用户的推荐列表
    val userRecs = preRatings
      .filter(_.rating > 0)
      .map(rating => (rating.user,(rating.product, rating.rating)))
      .groupByKey()
      .map{
        case (userId,recs) => UserRecs(userId,recs.toList.sortWith(_._2 >
          _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1,x._2)))
      }.toDF()
    userRecs.write
      .option("uri",mongoConfig.uri)
      .option("collection",USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 3. 利用商品的特征向量， 计算商品的相似度列表
    //获取商品的特征矩阵，数据格式 RDD[(scala.Int, scala.Array[scala.Double])]
    val productFeatures = model.productFeatures.map{case (productId,features) =>
      (productId, new DoubleMatrix(features))
    }
    // 计算余弦相似度，计算笛卡尔积并过滤合并
    val productRecs = productFeatures.cartesian(productFeatures)
      .filter{case (a,b) => a._1 != b._1}
      .map{case (a,b) =>
        val simScore = this.consinSim(a._2,b._2) // 求余弦相似度
        (a._1,(b._1,simScore))
      }.filter(_._2._2 > 0.6)
      .groupByKey()
      .map{case (productId,items) =>
        ProductRecs(productId,items.toList.map(x => Recommendation(x._1,x._2)))
      }.toDF()

    productRecs
      .write
      .option("uri", mongoConfig.uri)
      .option("collection",PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()


  }
  //计算两个商品之间的余弦相似度
  def consinSim(product1: DoubleMatrix, product2:DoubleMatrix) : Double ={
    product1.dot(product2) / ( product1.norm2()  * product2.norm2() )
  }
}
