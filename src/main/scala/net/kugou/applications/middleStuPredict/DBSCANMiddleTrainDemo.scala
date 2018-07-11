package net.kugou.applications.middleStuPredict

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import net.kugou.algorithms.dbscan.{Dbscan, DbscanModel, DbscanSettings, RawDataSet}
import net.kugou.algorithms.dbscan.util.io.IOHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: yhao
  * Time: 2018/07/02 10:24
  * Desc: 
  *
  */
object DBSCANMiddleTrainDemo extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf()
      .setAppName("simple dbscan demo")
      .setMaster("local[3]")
      .set("spark.driver.maxResultSize", "3g")

    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val data: DataFrame = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("data/jwd_sample_middle_new.csv")
    /*val data: DataFrame = spark.sql("select cast(trim(longtitude) as double) longitude, cast(trim(latidude) as double) latitude " +
      "from mllab.stu_jwd_userid_3")*/
    data.show(10)

    val rawDataSet: RawDataSet = IOHelper.dataTransform(data, "longitude", "latitude")

    val clusteringSettings: DbscanSettings = new DbscanSettings()
      .withEpsilon(0.0008)
      .withNumberOfPoints(15)
    val model: DbscanModel = Dbscan.train(rawDataSet, clusteringSettings)

    //    model.save("models/dbscanModel")

    println("Model Setting Summary: ")
    println("\tdistanceMeasure: " + model.settings.distanceMeasure)
    println("\tepsilon: " + model.settings.epsilon)
    println("\tnumberOfPoints: " + model.settings.numberOfPoints)
    println("\ttreatBorderPointsAsNoise: " + model.settings.treatBorderPointsAsNoise)

    println("\nclustered point sample: ")
    model.clusteredPoints.take(100).foreach(println)

    println("\nunclustered point sample: ")
    model.noisePoints.take(20).foreach(println)

    val clusteredBW: BufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("data/jwd_clustered_middle.dat")))
    val noiseBW: BufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("data/jwd_noise_collect_middle.dat")))
    model.clusteredPoints.sortBy(point => point.coordinates(0)).map(_.toString).collect().foreach(line => clusteredBW.write(line + "\r\n"))
    model.noisePoints.sortBy(point => point.coordinates(0)).map(_.toString).collect().foreach(line => noiseBW.write(line + "\r\n"))
    clusteredBW.close()
    noiseBW.close()

    spark.stop()
  }
}
