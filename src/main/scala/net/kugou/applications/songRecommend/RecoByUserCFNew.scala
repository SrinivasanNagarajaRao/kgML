package net.kugou.applications.songRecommend

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

/**
  * Author: yhao
  * Time: 2018/07/18 14:39
  * Desc: 
  *
  */
object RecoByUserCFNew extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val checkpointPath = "hdfs:/user/haoyan/checkpoint"
    val spark: SparkSession = SparkUtils().createSparkEnv(checkPointDir = checkpointPath)

    //    val sql: String = s"select userid, scid, rating from temp.tb_user_item_rating_2000"
//    val origData: DataFrame = spark.sql(sql)
    val sql: String = s"select userid, scid, rating from temp.tb_user_item_rating_20w_new where rating>10"


    spark.stop()
  }
}
