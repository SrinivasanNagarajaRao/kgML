package net.kugou.applications.collegePredict

import net.kugou.utils.SparkUtils
import net.kugou.utils.distance.GPSDistCalculator
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: yhao
  * Time: 2018/06/28 12:48
  * Desc: 
  *
  */
object CollectCollegePredictDemo extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    //加载人工收集高校中心点
    val collectSQL: String = "select jwd, distance from mllab.stu_jwd where jwd is not null and distance is not null"
    val collectMap: Map[Int, List[(Long, Double, Double, Double)]] = spark.sql(collectSQL).rdd.flatMap{ row =>
      val jwd: Array[Double] = row.getString(0).split("#").map(_.toDouble)
      val dist: Double = row.getLong(1).toDouble

      if (jwd.length > 1) Some((jwd(0), jwd(1), dist)) else None
    }.zipWithIndex().map(_.swap).map{record =>
      val index: Int = Math.floor(record._2._1).toInt

      (index, List((record._1, record._2._1, record._2._2, record._2._3)))
    }.reduceByKey(_ ::: _).collect().toMap

    val collectMapBC: Broadcast[Map[Int, List[(Long, Double, Double, Double)]]] = spark.sparkContext.broadcast(collectMap)

    //加载数据
    val sql: String = "select userid, cast(jd as double) jd, cast(wd as double) wd " +
      "from mllab.stu_user_addjwd_detail " +
      "where dt between '2018-03-01' and '2018-05-31' and userid is not null and jd<>'0' and wd<>'0'"

    val resultRDD = spark.sql(sql).rdd.mapPartitions{ iter =>
      val tmpMap: Map[Int, List[(Long, Double, Double, Double)]] = collectMapBC.value

      iter.map { row =>
        val userid: String = row.getString(0)
        val longitude: Double = row.getDouble(1)
        val latitude: Double = row.getDouble(2)
        val index: Int = Math.floor(longitude).toInt

        if (tmpMap.get(index).nonEmpty) {
          val centerList: List[(Long, Double, Double, Double)] = tmpMap(index)
          val clusterList = centerList.filter { item =>
            val dist: Double = GPSDistCalculator.getDistance(longitude, latitude, item._2, item._3)
            dist < item._4
          }

          if (clusterList.nonEmpty) (userid, 1L) else (userid, 0L)
        } else (userid, 0L)
      }
    }.reduceByKey(_ + _).filter(_._2 > 0)     //只保留有聚类的用户

    import spark.implicits._
    val result: DataFrame = resultRDD.toDF("userid", "count")
    result.createOrReplaceTempView("tmpTable")

    spark.sql("set hive.mapred.mode=nonstrict")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("use mllab")
    spark.sql("create table mllab.stu_user_jwd_collect as select userid, count from tmpTable")

    spark.stop()
  }
}
