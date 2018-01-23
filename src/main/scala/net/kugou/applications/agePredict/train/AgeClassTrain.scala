package net.kugou.applications.agePredict.train

import net.kugou.applications.Preprocessor
import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.reflect.io.File

/**
  *
  * Created by yhao on 2017/12/20 15:22.
  */
object AgeClassTrain extends Serializable {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val dm: String = args(0)
    val platform: String = args(1)
    val label: String = args(2)
    val iterNum: Int = args(3).toInt
    val sourceTable: String = args(4)
    val stopwordPath: String = args(5)
    val modelPath: String = args(6)

    //设置各特征向量维度
    val vocabSizeMap: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
    vocabSizeMap.put("applist", 10000)
    vocabSizeMap.put("songidlist", 15000)
    vocabSizeMap.put("singerlist", 5000)
    vocabSizeMap.put("nickname", 20000)
    vocabSizeMap.put("useralias", 20000)

    val processor: Preprocessor = new Preprocessor

    //加载数据SQL
    var sql: String = s"select userid, cast($label as int) as age, nickname, useralias, app_list, play_songid_list, top_singer, dm, pt " +
      s"from $sourceTable where dm='$dm' and $label is not null and (nickname is not null or useralias is not null or " +
      s"app_list is not null or play_songid_list is not null or top_singer is not null)"
    if (!platform.equalsIgnoreCase("all")) sql += s" and pt='$platform'"

    //加载数据
    val data: DataFrame = loadData(spark, sql)
    println(s"\n共加载数据：${data.count()}")

    //预处理，分词/去除停用词等
    val preData: DataFrame = processor.filterName(data, stopwordPath)

    //特征合并
    val fields: Array[String] = Array("filter_useralias", "applist", "songidlist", "singerlist")
    val vecData: DataFrame = processor.assemble(preData, fields, vocabSizeMap, modelPath).cache()
    println(s"\n合并特征后数据：${vecData.count()}")

    //去除零向量
    val nonZeroData: DataFrame = processor.filterEffectiveData(vecData, "features", 1)
    vecData.unpersist()

    //训练
    println(s"\n有效训练数据共: ${nonZeroData.count()} 条！")
    train(nonZeroData, "age", "features", "predictions", iterNum, modelPath)

    spark.stop()
  }


  def loadData(spark: SparkSession, sql: String, numPart: Int = 2000): DataFrame = {
    println(s"Hive查询SQL: $sql")

    import spark.implicits._
    val result: DataFrame = spark.sql(sql).rdd.map { row =>
      val userid: String = if(Option(row.get(0)).isEmpty) "" else row.getString(0)
      val age: Double = if(Option(row.get(1)).isEmpty) 5 else row.getInt(1).toDouble
      val nickname: String = if(Option(row.get(2)).isEmpty) "" else row.getString(2)
      val useralias: String = if(Option(row.get(3)).nonEmpty) row.getString(3) else nickname      //如果用户姓名不为空，则使用用户姓名，否则使用昵称

      val applist: Seq[String] = if(Option(row.get(4)).isEmpty) Seq() else row.getString(4).split(";").flatMap{appToken =>
        val tokens: Array[String] = appToken.split(",")
        if (tokens.length > 0) Some(tokens(0)) else None
      }

      val songidlist: Seq[String] = if(Option(row.get(5)).isEmpty) Seq() else row.getString(5).split(";").flatMap{songidToken =>
        val tokens: Array[String] = songidToken.split(":")
        if (tokens.length > 0) Some(tokens(0)) else None
      }

      val singerlist: Seq[String] = if(Option(row.get(6)).isEmpty) Seq() else row.getString(6).split(",").toSeq
      val dm: String = if(Option(row.getString(7)).isEmpty) "" else row.getString(7)
      val pt: String = if(Option(row.getString(8)).isEmpty) "" else row.getString(8)

      (userid, age, useralias, applist, songidlist, singerlist, dm, pt)
    }.toDF("userid", "age", "useralias", "applist", "songidlist", "singerlist", "dm", "pt")

    result.repartition(numPart)
  }


  def train(data: DataFrame, labelCol: String, featureCol: String,
            predictionCol: String, iterNum: Int, modelPath: String): LogisticRegressionModel = {
    val lr: LogisticRegression = new LogisticRegression()
      .setLabelCol(labelCol)
      .setFeaturesCol(featureCol)
      .setPredictionCol(predictionCol)
      .setMaxIter(iterNum)
    val model: LogisticRegressionModel = lr.fit(data)
    model.write.overwrite().save(modelPath + File.separator + "lrModel")

    model
  }
}
