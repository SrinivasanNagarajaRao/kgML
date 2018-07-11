package net.kugou.applications.collegePredict

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import net.kugou.algorithms.dbscan.{Dbscan, DbscanModel, DbscanSettings, RawDataSet}
import net.kugou.algorithms.dbscan.util.io.IOHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: yhao
  * Time: 2018/06/15 14:36
  * Desc: 从忠林聚合的常驻地表mllab.stu_jwd_userid_3导出数据作为数据源，计算高校中心点
  *
  */
object DBSCANCollegeTrainDemo extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setAppName("simple dbscan demo").setMaster("local[2]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val data: DataFrame = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("data/jwd_sample.csv")
    /*val data: DataFrame = spark.sql("select cast(trim(longtitude) as double) longitude, cast(trim(latidude) as double) latitude " +
      "from mllab.stu_jwd_userid_3")*/
    data.show(10)

    val rawDataSet: RawDataSet = IOHelper.dataTransform(data, "longitude", "latitude")

    val clusteringSettings: DbscanSettings = new DbscanSettings()
      .withEpsilon(0.0008)
      .withNumberOfPoints(15)
    val model: DbscanModel = Dbscan.train(rawDataSet, clusteringSettings)

    model.save("models/dbscanModel")

    println("Model Setting Summary: ")
    println("\tdistanceMeasure: " + model.settings.distanceMeasure)
    println("\tepsilon: " + model.settings.epsilon)
    println("\tnumberOfPoints: " + model.settings.numberOfPoints)
    println("\ttreatBorderPointsAsNoise: " + model.settings.treatBorderPointsAsNoise)

    println("\nclustered point sample: ")
    model.clusteredPoints.take(100).foreach(println)

    println("\nunclustered point sample: ")
    model.noisePoints.take(20).foreach(println)

    val clusteredBW: BufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("data/jwd_clustered.dat")))
    val noiseBW: BufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("data/jwd_noise.dat")))
    model.clusteredPoints.sortBy(point => point.coordinates(0)).map(_.toString).collect().foreach(line => clusteredBW.write(line + "\r\n"))
    model.noisePoints.sortBy(point => point.coordinates(0)).map(_.toString).collect().foreach(line => noiseBW.write(line + "\r\n"))
    clusteredBW.close()
    noiseBW.close()

    spark.stop()
  }
}
