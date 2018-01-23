package net.kugou.applications.agePredict.predict

import net.kugou.algorithms.MultiClassifications
import net.kugou.applications.Preprocessor
import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.reflect.io.File

/**
  *
  * Created by yhao on 2017/12/20 17:53.
  */
object AgeClassPredict extends Serializable {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val dm: String = args(0)
    val platform: String = args(1)
    val sourceTable: String = args(2)
    val resultTable: String = args(3)
    val stopwordPath: String = args(4)
    val modelPath: String = args(5)

    val processor: Preprocessor = new Preprocessor

    var sql: String = s"select userid, nickname, useralias, app_list, play_songid_list, top_singer, dm, pt " +
      s"from $sourceTable where dm='$dm' and (nickname is not null or useralias is not null or " +
      s"app_list is not null or play_songid_list is not null or top_singer is not null)"
    if (!platform.equalsIgnoreCase("all")) sql += s" and pt='$platform'"

    //加载数据
    val data: DataFrame = loadData(spark, sql)

    //预处理，分词/去除停用词等
    val preData: DataFrame = processor.filterName(data, stopwordPath)

    //特征合并
    val fields: Array[String] = Array("filter_useralias", "applist", "songidlist", "singerlist")
    val vecData: DataFrame = processor.assemble(preData, fields, modelPath)

    //去除零向量
    val nonZeroData: DataFrame = processor.filterEffectiveData(vecData, "features", 1)

    //预测
    val predictions: DataFrame = predict(nonZeroData, modelPath)

    //保存结果
    import spark.implicits._
    val result = predictions.select("userid", "predictions", "probability", "dm", "pt").rdd.map{row =>
      val proArray: Array[Double] = row.getAs[Vector]("probability").toArray
      val pro_before_70: Double = proArray(0)     //70前概率
      val pro_after_70: Double = proArray(1)      //70后概率
      val pro_after_80: Double = proArray(2)      //80后概率
      val pro_after_90: Double = proArray(3)      //90后概率
      val pro_after_00: Double = proArray(4)      //00后概率
      (row.getString(0), row.getDouble(1).toInt, pro_before_70, pro_after_70, pro_after_80, pro_after_90, pro_after_00, row.getString(3), row.getString(4))
    }.toDF("userid", "age_pre_part", "pro_before_70", "pro_after_70", "pro_after_80", "pro_after_90", "pro_after_00", "dm", "pt")

    result.createOrReplaceTempView("tmpTable")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val createTableSQL: String = s"create table if not exists $resultTable(" +
      s"userid string COMMENT '用户ID', " +
      s"age_pre_part int COMMENT '预测年龄段', " +
      s"pro_before_70 double COMMENT '70前概率', " +
      s"pro_after_70 double COMMENT '70后概率', " +
      s"pro_after_80 double COMMENT '80后概率', " +
      s"pro_after_90 double COMMENT '90后概率', " +
      s"pro_after_00 double COMMENT '00后概率' " +
      s")PARTITIONED BY (dm string, pt string) " +
      s"ROW FORMAT DELIMITED  " +
      s"FIELDS TERMINATED BY '|'  STORED AS TEXTFILE"

    val insertSQL: String = s"INSERT OVERWRITE TABLE $resultTable partition(dm, pt) select * from tmpTable where dm='$dm'"

    spark.sql(createTableSQL)
    spark.sql(insertSQL)

    spark.stop()
  }


  def loadData(spark: SparkSession, sql: String, numPart: Int = 2000): DataFrame = {
    println(s"Hive查询SQL: $sql")

    import spark.implicits._
    val result: DataFrame = spark.sql(sql).rdd.map { row =>
      val userid: String = if(Option(row.get(0)).isEmpty) "" else row.getString(0)
      val nickname: String = if(Option(row.get(1)).isEmpty) "" else row.getString(1)
      val useralias: String = if(Option(row.get(2)).nonEmpty) row.getString(2) else nickname      //如果用户姓名不为空，则使用用户姓名，否则使用昵称

      val applist: Seq[String] = if(Option(row.get(3)).isEmpty) Seq() else row.getString(3).split(";").flatMap{appToken =>
        val tokens: Array[String] = appToken.split(",")
        if (tokens.length > 0) Some(tokens(0)) else None
      }

      val songidlist: Seq[String] = if(Option(row.get(4)).isEmpty) Seq() else row.getString(4).split(";").flatMap{songidToken =>
        val tokens: Array[String] = songidToken.split(":")
        if (tokens.length > 0) Some(tokens(0)) else None
      }

      val singerlist: Seq[String] = if(Option(row.get(5)).isEmpty) Seq() else row.getString(5).split(",").toSeq
      val dm: String = if(Option(row.getString(6)).isEmpty) "" else row.getString(6)
      val pt: String = if(Option(row.getString(7)).isEmpty) "" else row.getString(7)

      (userid, useralias, applist, songidlist, singerlist, dm, pt)
    }.toDF("userid", "useralias", "applist", "songidlist", "singerlist", "dm", "pt")

    result.repartition(numPart)
  }


  def predict(data: DataFrame, modelPath: String): DataFrame = {
    val model: LogisticRegressionModel = LogisticRegressionModel.load(modelPath + File.separator + "lrModel")
    model.transform(data)
  }


  def evaluate(spark: SparkSession, data: DataFrame) = {
    val classifications = new MultiClassifications(spark)
    val predictionRDD: RDD[(Double, Double)] = data.select("predictions", "age").rdd.map { case Row(predicition: Double, age: Double) => (predicition, age) }

    predictionRDD.map(_.swap).groupByKey().map { record =>
      var tmpCount: Int = 0
      val count: Int = record._2.size
      record._2.foreach(pred => if (pred == record._1) {
        tmpCount += 1
      })
      (record._1, tmpCount, count)
    }.sortBy(_._1).collect().foreach { record =>
      println(s"年龄段：${record._1}, 准确率：${(1.0 * record._2 / record._3) * 100}%, 预测准确数：${record._2}, 总数：${record._3}")
    }

    classifications.evaluate(predictionRDD)
  }
}
