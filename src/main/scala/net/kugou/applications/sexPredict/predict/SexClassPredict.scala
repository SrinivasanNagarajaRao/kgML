package net.kugou.applications.sexPredict.predict

import net.kugou.applications.Preprocessor
import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.io.File

/**
  *
  * Created by yhao on 2017/12/26 16:43.
  */
object SexClassPredict extends Serializable {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val date: String = args(0)
    var dm: String = args(1)
    val platform: String = args(2)
    val sourceTable: String = args(3)
    val resultTable: String = args(4)
    val stopwordPath: String = args(5)
    val modelPath: String = args(6)

    if (dm.isEmpty) {if(date.length > 7) dm = date.substring(0, 7) else dm = date}

    val processor: Preprocessor = new Preprocessor

    var sql: String = s"select userid, nickname, useralias, app_list, play_songid_list, top_singer, '$date' as dt, pt " +
      s"from $sourceTable where dm='$dm' and (nickname is not null or useralias is not null or " +
      s"app_list is not null or play_songid_list is not null or top_singer is not null)"
    if (!platform.equalsIgnoreCase("all")) sql += s" and pt='$platform'"

    //加载数据
    val data: DataFrame = loadData(spark, sql)
    println(s"\n共加载数据：${data.count()}")

    //预处理，分词/去除停用词等
    val preData: DataFrame = processor.filterName(data, stopwordPath)

    //特征合并
    val fields: Array[String] = Array("filter_useralias", "applist", "songidlist", "singerlist")
    val vecData: DataFrame = processor.assemble(preData, fields, modelPath)
    println(s"s\n合并特征后数据条数：${vecData.count()}")

    //去除零向量
    val nonZeroData: DataFrame = processor.filterEffectiveData(vecData, "features", 1)

    //预测
    println(s"\n有效预测数据共: ${nonZeroData.count()} 条！")
    val predictions: DataFrame = predict(nonZeroData, modelPath)

    //保存结果
    import spark.implicits._
    val result = predictions.select("userid", "predictions", "probability", "dt", "pt").rdd.map{row =>
      val proArray: Array[Double] = row.getAs[Vector]("probability").toArray
      val pro_nan: Double = proArray(0)     //男性概率
      val pro_woman: Double = proArray(1)      //女性概率
      (row.getString(0), row.getDouble(1).toInt, pro_nan, pro_woman, row.getString(3), row.getString(4))
    }.toDF("userid", "sex_pre", "pro_nan", "pro_woman", "dt", "pt")


    result.createOrReplaceTempView("tmpTable")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val createTableSQL: String = s"create table if not exists $resultTable(" +
      s"userid string COMMENT '用户ID', " +
      s"sex_pre int COMMENT '预测性别', " +
      s"pro_nan double COMMENT '男性概率', " +
      s"pro_woman double COMMENT '女性概率'" +
      s")PARTITIONED BY (dt string, pt string) " +
      s"ROW FORMAT DELIMITED  " +
      s"FIELDS TERMINATED BY '|'  STORED AS TEXTFILE"

    val insertSQL: String = s"INSERT INTO $resultTable partition(dt, pt) select * from tmpTable where dt='$date'"

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
      val dt: String = if(Option(row.getString(6)).isEmpty) "" else row.getString(6)
      val pt: String = if(Option(row.getString(7)).isEmpty) "" else row.getString(7)

      (userid, useralias, applist, songidlist, singerlist, dt, pt)
    }.toDF("userid", "useralias", "applist", "songidlist", "singerlist", "dt", "pt")

    result.repartition(numPart)
  }


  def predict(data: DataFrame, modelPath: String): DataFrame = {
    val model: LogisticRegressionModel = LogisticRegressionModel.load(modelPath + File.separator + "lrModel")
    model.transform(data)
  }
}
