package net.kugou.applications.middleStuPredict

import net.kugou.utils.SparkUtils
import net.kugou.utils.distance.GPSDistCalculator
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: yhao
  * Time: 2018/07/02 9:26
  * Desc: 
  *
  */
object DBSCANMiddlePredictDemo extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val centersFilePath: String = "hdfs://kgdc/user/haoyan/data/centers_middle.dat"

    // dbscan聚类中心点
    val centerData: RDD[String] = spark.sparkContext.textFile(centersFilePath)
    val centerMap = centerData.flatMap{line =>
      val tokens: Array[String] = line.split(",").map(_.trim)
      if (tokens.length > 4) {
        val clusterId: Long = tokens(0).toLong
        val longitude: Double = tokens(1).toDouble
        val latitude: Double = tokens(2).toDouble
        val dist: Double = tokens(3).toDouble
        val count: Long = tokens(4).toLong

        val index: Int = Math.floor(longitude).toInt

        if (count > 12 && (dist/count) <= 3) Some((index, List((clusterId, longitude, latitude, dist, count)))) else None
      } else None
    }.reduceByKey(_ ::: _).collect().toMap

    val sql: String = "select userid, cast(jd as double) jd, cast(wd as double) wd, address, dt, pt " +
      "from mllab.stu_user_addjwd_detail " +
      "where dt between '2018-05-01' and '2018-05-31' and userid is not null and jd<>'0' and wd<>'0'"

    val resultRDD = spark.sql(sql).rdd.map{row =>
      val userid: String = row.getString(0)
      val longitude: Double = row.getDouble(1)
      val latitude: Double = row.getDouble(2)
      val address: String = row.getString(3)
      val dt: String = row.getString(4)
      val pt: String = row.getString(5)

      val index: Int = Math.floor(longitude).toInt

      if (centerMap.get(index).nonEmpty) {
        val centerList: List[(Long, Double, Double, Double, Long)] = centerMap(index)
        val clusterList = centerList.filter{item =>
          val dist: Double = GPSDistCalculator.getSimpleDistance(longitude, latitude, item._2, item._3)
          dist < item._4
        }

        val clusterId: Long = if (clusterList.nonEmpty) clusterList.maxBy(_._5)._1 else 0L
        (userid, longitude, latitude, clusterId, address, dt, pt)
      } else (userid, longitude, latitude, 0L, address, dt, pt)
    }

    import spark.implicits._
    val result: DataFrame = resultRDD.toDF("userid", "longitude", "latitude", "clusterId", "address", "dt", "pt")
    result.createOrReplaceTempView("tmpTable")

    spark.sql("set hive.mapred.mode=nonstrict")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("use mllab")
    spark.sql("insert overwrite table mllab.stu_user_jwd_middle_detail_d partition(dt, pt) " +
      "select userid, longitude, latitude, clusterId cluster_id, address, dt, pt from tmpTable")


    spark.stop()
  }
}
