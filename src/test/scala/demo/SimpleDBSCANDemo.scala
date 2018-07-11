package demo

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import net.kugou.algorithms.dbscan.{Dbscan, DbscanModel, DbscanSettings, RawDataSet}
import net.kugou.algorithms.dbscan.util.io.IOHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: yhao
  * Time: 2018/06/15 14:36
  * Desc: 
  *
  */
object SimpleDBSCANDemo extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setAppName("simple dbscan demo").setMaster("local[2]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    /*val data: DataFrame = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("data/jwd_sample.csv")*/
    val data: DataFrame = spark.sql("select cast(trim(longtitude) as double) longitude, cast(trim(latidude) as double) latitude " +
      "from mllab.stu_jwd_userid_3")
    data.show(10)

    val rawDataSet: RawDataSet = IOHelper.dataTransform(data, "longitude", "latitude")

    val clusteringSettings: DbscanSettings = new DbscanSettings().withEpsilon(0.001).withNumberOfPoints(15)
    val model: DbscanModel = Dbscan.train(rawDataSet, clusteringSettings)

    println("clustered point sample: ")
    model.clusteredPoints.take(100).foreach(println)

    println("unclustered point sample: ")
    model.noisePoints.take(20).foreach(println)

    /*val clusteredBW: BufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("data/jwd_clustered.dat")))
    val noiseBW: BufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("data/jwd_noise.dat")))
    model.clusteredPoints.map(_.toString).collect().foreach(line => clusteredBW.write(line + "\n"))
    model.noisePoints.map(_.toString).collect().foreach(line => noiseBW.write(line + "\n"))
    clusteredBW.close()
    noiseBW.close()*/

    spark.stop()
  }
}
