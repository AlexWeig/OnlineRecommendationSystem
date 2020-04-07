package cn.edu.neu.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

case class MongoConfig(uri:String, db:String)

object StatRecommender {
  //定义mongodb的表名
  val MONGODB_RATING_COLLECTION = "Rating"

  //统计的表的名称
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Software\\playfootball-winutils-master\\winutils\\hadoop-2.7.1")
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/erecommender",
      "mongo.db" -> "erecommender"
    )
    //创建SparkConf配置
    val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))
    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()



    //加入隐式转换
    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    //数据加载进来
    val ratingDF = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    //创建一张名叫ratings的表
    ratingDF.createOrReplaceTempView("ratings")
    //TODO: 不同的统计推荐结果
    //1. 历史热门商品，按照评分个数统计
    //数据结构 -》  productId,count
    val rateMoreProductsDF = spark.sql("select productId, count(productId) as count from ratings group by productId order by count")
    storeDFInMongoDB(rateMoreProductsDF,RATE_MORE_PRODUCTS)

    //2. 近期热门商品，把时间戳转换成yyyyMM格式进行评分
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // 注册UDF，将timestamp转化为年月格式yyyyMM
    spark.udf.register("changeDate",(x:Int)=>simpleDateFormat.format(new Date(x * 1000L)).toInt)
    // 把原始rating数据转换成想要的结构productId, score, yearmonth
    val ratingOfYearMonthDF = spark.sql("select productId, score, changeDate(timestamp) as yearmonth from ratings ")
    // 将新的数据集注册成为一张表
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")

    val rateMoreRecentlyProducts = spark.sql("select productId, count(productId) as count ,yearmonth from ratingOfMonth group by yearmonth,productId order by yearmonth desc, count desc")
    storeDFInMongoDB(rateMoreRecentlyProducts,RATE_MORE_RECENTLY_PRODUCTS)
    //3. 优质商品统计，商品的平均评分, productID, avg
    val averageProductDF = spark.sql("select productId, avg(score) as avg from ratings group by productId order by avg desc")
    storeDFInMongoDB(averageProductDF,AVERAGE_PRODUCTS)
    spark.stop()
  }

  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
      df.write
        .option("uri",mongoConfig.uri)
        .option("collection",collection_name)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()

  }




}
