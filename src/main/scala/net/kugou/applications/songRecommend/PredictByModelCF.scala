package net.kugou.applications.songRecommend

import net.kugou.utils.{ExtMatrixFactorizationModelHelper, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * Author: yhao
  * Time: 2018/07/14 20:19
  * Desc: 
  *
  */
object PredictByModelCF extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val modelPath: String = args(0)
    val resultTable: String = args(1)

    val checkpointPath = "hdfs:/user/haoyan/checkpoint"
    val spark: SparkSession = SparkUtils().createSparkEnv(checkPointDir = checkpointPath)

    val model = ALSModel.load(modelPath)
//    val predictor: CFPredictor = new CFPredictor(spark)
//    val result: DataFrame = predictor.recommendForAllUsers(model, 50)

    val resultRDD: RDD[(Int, Array[(Int, Double)])] = ExtMatrixFactorizationModelHelper.recommendForAll(
      model.rank, model.userFactors, model.itemFactors, 100, 256000, StorageLevel.MEMORY_AND_DISK_SER)


    import spark.implicits._
    val result: DataFrame = resultRDD.map{record =>
      val userid: Int = record._1
      val ratingStr: String = record._2.map(pair => pair._1 + "," + pair._2).mkString(";")
      (userid, ratingStr)
    }.toDF("userid", "recommendations")

    result.createOrReplaceTempView("tmpTable")
    spark.sql("use temp")
    spark.sql(s"create table $resultTable as select userid, recommendations from tmpTable")
//    spark.sql(s"create table temp.tb_user_item_ALS_100w_result as select userid, recommendations from tmpTable")

    spark.stop()
  }
}
