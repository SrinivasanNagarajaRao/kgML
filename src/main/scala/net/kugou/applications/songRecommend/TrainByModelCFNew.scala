package net.kugou.applications.songRecommend

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: yhao
  * Time: 2018/07/11 14:24
  * Desc: 
  *
  */
object TrainByModelCFNew extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val srcTable: String = args(0)
    val scidTable: String = args(1)
    val modelPath: String = args(2)
    val partitions: Int = args(3).toInt

    val checkpointPath = "hdfs:/user/haoyan/checkpoint"
    val spark: SparkSession = SparkUtils().createSparkEnv(checkPointDir = checkpointPath)

    val ratingSQL: String = s"select userid, scid, rating from $srcTable"
    val userItemRatingData: DataFrame = spark.sql(ratingSQL)

    val scidSQL: String = s"select cast(scid as int) scid from $scidTable group by scid"
    val scidData: DataFrame = spark.sql(scidSQL)

    val data: DataFrame = scidData.join(userItemRatingData, "scid").repartition(partitions)

    val splits: Array[Double] = Array(Double.NegativeInfinity, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, Double.PositiveInfinity)
    val bucketizer: Bucketizer = new Bucketizer()
      .setHandleInvalid("skip")
      .setSplits(splits)
      .setInputCol("rating")
      .setOutputCol("rating_bin")
    val dataBin: DataFrame = bucketizer.transform(data)

    val als: ALS = new ALS()
      .setNumBlocks(1000)
      .setRegParam(0.01)
      .setMaxIter(50)
      .setRank(20)
      .setAlpha(1.0)
      .setUserCol("userid")
      .setItemCol("scid")
      .setRatingCol("rating_bin")
    val alsModel: ALSModel = als.fit(dataBin)
    alsModel.write.overwrite().save(modelPath)

    spark.stop()
  }
}
