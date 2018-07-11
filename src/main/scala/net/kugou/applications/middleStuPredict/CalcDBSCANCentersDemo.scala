package net.kugou.applications.middleStuPredict

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import net.kugou.utils.distance.GPSDistCalculator
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Author: yhao
  * Time: 2018/07/02 10:22
  * Desc: 根据
  *
  */
object CalcDBSCANCentersDemo extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setAppName("calc dbscan center").setMaster("local[2]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val data: RDD[(Long, List[(Long, Double, Double, Long)])] = spark.sparkContext.textFile{"data/jwd_clustered_middle.dat"}.flatMap{line =>
      val tokens: Array[String] = line.split(";").map(_.trim)

      if (tokens.length > 4) {
        val jwd: Array[String] = tokens(0).split("=")(1).trim.split(",").map(_.trim)
        val longitude: Double = jwd(0).toDouble
        val latitude: Double = jwd(1).toDouble
        val id: Long = tokens(1).split("=")(1).trim.toLong
        val box: Long = tokens(2).split("=")(1).trim.toLong
        val cluster: Long = tokens(3).split("=")(1).trim.toLong
        val neighborCnt: Long = tokens(4).split("=")(1).trim.toLong

        Some((cluster, List((id, longitude, latitude, neighborCnt))))
      } else None
    }

    val clusterCntMap: collection.Map[Long, Long] = data.countByKey()

    val result = data.reduceByKey(_ ::: _).map{record =>
      val cluster: Long = record._1
      val count: Long = clusterCntMap(cluster)
      val jwdList: List[(Double, Double)] = record._2.map(item => (item._2, item._3))
      var avgLongitude: Double = jwdList.map(_._1).sum / count
      var avgLatitude: Double = jwdList.map(_._2).sum / count

      avgLongitude = 1.0 * (avgLongitude * 1000000).toInt / 1000000
      avgLatitude = 1.0 * (avgLatitude * 1000000).toInt / 1000000

      var avgDist: Double = jwdList.map{
        pair => GPSDistCalculator.getDistance(pair._1, pair._2, avgLongitude, avgLatitude)
      }.sum / count

      avgDist = 1.0 * (avgDist * 10000).toInt / 10000   //保留4为小数

      (cluster, avgLongitude, avgLatitude, avgDist, count)
    }.sortBy(_._5, ascending = false)

    val bw: BufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("data/centers_middle.dat")))

    result.map{record =>
      record._1 + ", " + record._2 + ", " + record._3 + ", " + record._4 + ", " + record._5
    }.collect().foreach{line =>
      bw.write(line + "\r\n")
    }

    bw.close()
    spark.stop()
  }
}
