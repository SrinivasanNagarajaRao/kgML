package net.kugou.applications.agePredict.predict

import net.kugou.algorithms.MultiClassifications
import net.kugou.applications.agePredict.train.TrainWithAll
import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.reflect.io.File

/**
  *
  * Created by yhao on 2017/12/12 10:48.
  */
object RegPredictWithAll extends Serializable {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val platform: String = args(0)
    val minFeatureNum: Int = args(2).toInt
//    val label: String = args(3)
    val sourceTable: String = args(4)
    val resultTable: String = args(5)
    val stopwordPath: String = args(6)
    val modelPath: String = args(7)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val userid: String = "userid"
    val dm: String = "2017-09"

    //数值型字段
    val numericFields: Array[String] = Array("age_avg", "cnt_start", "cnt_login", "cnt_play_audio", "cnt_scid", "cnt_play_audio_lib",
      "cnt_play_audio_radio", "cnt_play_audio_search", "cnt_play_audio_favorite", "cnt_play_audio_list",
      "cnt_play_audio_like", "cnt_play_audio_recommend", "cnt_play_video", "cnt_download_audio", "cnt_search",
      "cnt_search_valid", "cnt_favorite_scid", "cnt_favorite_list", "cnt_share", "cnt_comment", "usr_follow", "usr_fan",
      "keep_pay_month", "his_musicpack_buy_cnt", "his_musicpack_free_cnt", "year_musicpack_free_cnt",
      "first_musicpack_buy_valid_days", "last_musicpack_buy_apart_days", "his_svip_buy_cnt", "last_svip_buy_valid_days",
      "single_buy_cnt", "year_album_buy_cnt", "year_album_buy_times", "deductpoint_cnt")

    //文本型字段
    var textFields: Array[String] = Array("nickname", "useralias")

    //列表型字段
    val listFields: Array[String] = Array("app_list", "play_songid_list", "top_singer")

    //构建SQL
    var sql: String = s"select distinct $userid, dm, pt "

    if (numericFields.nonEmpty) sql += s", ${numericFields.mkString(", ")} "
    if (textFields.nonEmpty) sql += s", ${textFields.mkString(", ")} "
    if (listFields.nonEmpty) sql += s", ${listFields.mkString(", ")} "

    sql += s" from $sourceTable where dm='$dm' "
//    if (listFields.nonEmpty) sql += listFields.map(field => " and " + field + " is not null").mkString(" ")
    /*if (listFields.nonEmpty) sql += listFields.flatMap{field =>
      if (!field.equals("app_list")) Some(" and " + field + " is not null") else None
    }.mkString(" ")*/
    if (!platform.equalsIgnoreCase("all")) sql += s" and pt='$platform'"

    //加载数据
    val data: DataFrame = PredictWithAll.loadData(spark, sql, numericFields, textFields, listFields)
    println(s"\n共加载数据：${data.count()}")

    //删除nickname字段(已与useralias字段合并)
    textFields = textFields.dropWhile(_.equals("nickname"))

    //预处理
    val (preData, vecFieldNames, vocabSizeMap) = TrainWithAll.preprocess(data, textFields, listFields, stopwordPath)

    //向量化与聚合特征
    val vecData: DataFrame = PredictWithAll.assemble(preData, numericFields, vecFieldNames, minFeatureNum, vocabSizeMap, modelPath)

    //预测结果
    println(s"\n有效预测数据共: ${vecData.count()} 条！")
    val predictions: DataFrame = predict(vecData, modelPath)

    //保存结果
    predictions.select("userid", "predictions", "dm", "pt").distinct().write.mode(SaveMode.Overwrite).insertInto(resultTable)

    spark.stop()
  }


  def predict(data: DataFrame, modelPath: String): DataFrame = {
    val model: LinearRegressionModel = LinearRegressionModel.load(modelPath + File.separator + "all_lrModel")
    model.transform(data)
  }


  def evaluate(spark: SparkSession, data: DataFrame) = {
    val classifications = new MultiClassifications(spark)
    val predictionRDD: RDD[(Double, Double)] = data.select("predictions", "age").rdd.map{case Row(predicition: Double, age: Double) => (predicition, age)}

    predictionRDD.map(_.swap).groupByKey().map{record =>
      var tmpCount: Int = 0
      val count: Int = record._2.size
      record._2.foreach(pred => if(pred == record._1){tmpCount += 1})
      (record._1, tmpCount, count)
    }.sortBy(_._1).collect().foreach{record =>
      println(s"年龄段：${record._1}, 准确率：${(1.0 * record._2 / record._3) * 100}%, 预测准确数：${record._2}, 总数：${record._3}")
    }

    classifications.evaluate(predictionRDD)
  }
}
