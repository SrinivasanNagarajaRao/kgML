package demo.pipeline

import net.kugou.pipeline.Segmenter
import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test

/**
  *
  * Created by yhao on 2017/12/11 12:35.
  */
class SegmenterDemo {
  val logger: Logger = Logger.getLogger(this.getClass)
  Logger.getLogger("org").setLevel(Level.WARN)

  val spark: SparkSession = SparkUtils().createSparkEnv("local")
  val sc: SparkContext = spark.sparkContext

  @Test
  def segTypeTest(): Unit ={


    val text: Seq[String] = Seq(
      "广东省广州市海珠区新港西路135号中山大学，数学与计算科学学院信息与计算科学专业学生张三，宿舍位于园东区170栋~181栋"
    )

    import spark.implicits._
    val data: DataFrame = sc.parallelize(text).toDF("text")

    val segmenter: Segmenter = new Segmenter()
      .setInputCol("text")
      .setOutputCol("text_seg")

    val segTypes: Array[String] = Array("StandardSegment", "NLPSegment", "IndexSegment", "SpeedSegment", "NShortSegment", "CRFSegment")
    for (segType <- segTypes) {
      segmenter.setSegType(segType)
      val segedData: DataFrame = segmenter.transform(data)
      println(s"\n$segType 分词结果：")
      segedData.collect().foreach(println)
    }
  }
}
