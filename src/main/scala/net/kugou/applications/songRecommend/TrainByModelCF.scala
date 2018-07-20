package net.kugou.applications.songRecommend

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{Bucketizer, QuantileDiscretizer}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import net.kugou.applications.songRecommend.CFPredictor

/**
  * Author: yhao
  * Time: 2018/07/11 14:24
  * Desc: 
  *
  */
object TrainByModelCF extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

//    val srcTable: String = "temp.tb_user_item_rating_10000w"
//    val modelPath: String = "hdfs://kgdc/user/haoyan/models/recommend/model10000w"
//    val resultTable: String = "temp.tb_user_item_ALS_10000w_result"
    val srcTable: String = args(0)
    val modelPath: String = args(1)
    val resultTable: String = args(2)

    val checkpointPath = "hdfs:/user/haoyan/checkpoint"
    val spark: SparkSession = SparkUtils().createSparkEnv(checkPointDir = checkpointPath)

//    val sql: String = "select userid, count, scidlist from temp.tmp_cds_20180707_user_list_filter where count>20 and count<800 limit 10000"
//    val userItemRatingData: DataFrame = loadData(spark, sql)
//    userItemRatingData.createOrReplaceTempView("tmpTable")
//    spark.sql("use temp")
//    spark.sql("create table temp.tb_user_item_rating_10000w as select userid, scid, rating from tmpTable order by rand(1234)")

    val sql: String = s"select userid, scid, rating from $srcTable"
    val userItemRatingData: DataFrame = spark.sql(sql)

    val splits: Array[Double] = Array(Double.NegativeInfinity, -200, -100, -75, -50, -40, -30, -20, -18, -16,
    -14, -12, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 30,
      40, 50, 75, 100, 125, 150, 175, 200, 250, 300, 350, 400, 500, 600, 800, 1000, 2000, Double.PositiveInfinity)
    val bucketizer: Bucketizer = new Bucketizer()
      .setHandleInvalid("skip")
      .setSplits(splits)
      .setInputCol("rating")
      .setOutputCol("rating_bin")
    val userItemRatingBinData: DataFrame = bucketizer.transform(userItemRatingData)

    val als: ALS = new ALS()
      .setNumBlocks(1000)
      .setRegParam(0.01)
      .setMaxIter(50)
      .setRank(20)
      .setAlpha(1.0)
      .setUserCol("userid")
      .setItemCol("scid")
      .setRatingCol("rating_bin")
    val alsModel: ALSModel = als.fit(userItemRatingBinData)
    alsModel.write.overwrite().save(modelPath)

    spark.stop()
  }
}
