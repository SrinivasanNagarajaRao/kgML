package net.kugou.utils

import org.apache.hadoop.fs.Path
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
  def createSparkEnv(master: String = "", checkPointDir: String = ""): SparkSession = {
    val conf: SparkConf = new SparkConf()
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.scheduler.mode", "FIFO")
      .set("spark.driver.maxResultSize", "10g")
//      .set("spark.speculation", "true")

    if (master.nonEmpty) conf.setMaster(master)

    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    if (checkPointDir.nonEmpty) {
      val path = new Path(checkPointDir)
      val sc = spark.sparkContext
      val hadoopConf = sc.hadoopConfiguration
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      if(hdfs.exists(path)){
        hdfs.delete(path,true)
        println(s"成功删除 $checkPointDir")
      }
      sc.setCheckpointDir(checkPointDir)
    }

    spark
  }
}

object SparkUtils extends Serializable {
  def apply(): SparkUtils = new SparkUtils()
}