package demo.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
  * Author: yhao
  * Time: 2018/06/14 19:25
  * Desc: 
  *
  */
object SampleDemo extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setAppName("sample demo").setMaster("local")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


  }


  /**
    * 按指定比例对数据进行随机抽样
    * @param lines  输入数据
    * @param threshold  抽样比例
    * @return
    */
  def randomSample1(lines: RDD[String], threshold: Double = 0.1): RDD[String] = {
    lines.filter{_ =>
      Random.nextDouble()<threshold
    }
  }

  def randomSample2(lines: RDD[String], k: Int): RDD[String] = {
    //TODO
    null
  }
}
