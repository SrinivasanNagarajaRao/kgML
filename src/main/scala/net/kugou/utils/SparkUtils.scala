package net.kugou.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *
  * Created by yhao on 2017/12/11 12:40.
  */
class SparkUtils extends Serializable {


  /**
    * 构建spark执行环境
    */
  def createSparkEnv(master: String = ""): SparkSession = {

    val conf: SparkConf = new SparkConf()
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.scheduler.mode", "FIFO")
      .set("spark.driver.maxResultSize", "10g")
//      .set("spark.speculation", "true")

    if (master.nonEmpty) conf.setMaster(master)

    SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
  }
}

object SparkUtils extends Serializable {
  def apply(): SparkUtils = new SparkUtils()
}