package net.kugou.applications.undergraduatePredict

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature.{CountVectorizerModel, VectorAssembler}
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: yhao
  * Time: 2018/06/13 9:47
  * Desc: 
  *
  */
object StuClassPredictPre extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val modelPath: String = "hdfs://kgdc/user/haoyan/models/college_class_pre/"

    //加载数据
    val data: DataFrame = loadTestData(spark)

    //文本特征向量化
    val cvModel: CountVectorizerModel = CountVectorizerModel.load(modelPath + "cvModel")
    val vecData: DataFrame = cvModel.transform(data)

    //聚合特征列
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("app_vec", "stuType", "jwdVec"))
      .setOutputCol("features")
    val assembleData: DataFrame = assembler.transform(vecData)

    //预测数据
    val model: NaiveBayesModel = NaiveBayesModel.load(modelPath + "nbModel")
    val predictions: DataFrame = model.transform(assembleData)

    import spark.implicits._
    val resultData: DataFrame = predictions.select("userid", "prediction", "probability").rdd.map{row =>
      val userid: String = row.getString(0)
      val prediction: Double = row.getDouble(1)
      val probability: Array[Double] = row.getAs[Vector](2).toArray
      val prob_0: Double = probability(0)
      val prob_1: Double = probability(1)

      (userid, prediction, prob_0, prob_1)
    }.toDF("userid", "prediction", "prob_0", "prob_1")

    resultData.createOrReplaceTempView("tmpTable")

    spark.sql("use mllab")
    spark.sql("CREATE TABLE mllab.userid_predict_for_train AS select userid, prediction, prob_0, prob_1 from tmpTable")

    spark.stop()
  }


  def loadTestData(spark: SparkSession): DataFrame = {
    val userSQL: String = "select t1.userid, t1.app_list " +
      "from (" +
      "    select userid, app_list from dsl.dwf_userinfo_digging_3month_d where dt='2018-05-31' and app_list is not null" +
      ") t1 left outer join (" +
      "    select userid from temp.sljin_diaoyan_msgid where type='all_rand_1'" +
      ") t2 on t1.userid=t2.userid " +
      "where t2.userid is null"

    val citySQL: String = "select userid, is_student from temp.stu_result_city_new"
    val jwdSQL: String = "select userid, jwd0, jwd100, jwd200, jwd500, jwd1000, jwd0days, jwd100days, jwd200days, jwd500days, jwd1000days from mllab.stu_result_userid3"
    println(s"Load user data sql: \n\t$userSQL")
    println(s"Load city data sql: \n\t$citySQL")
    println(s"Load jwd data sql: \n\t$jwdSQL")

    val userData: DataFrame = spark.sql(userSQL)    //292005575
    val cityData: DataFrame = spark.sql(citySQL)    //205486889
    val jwdData: DataFrame = spark.sql(jwdSQL)      //62608264

    val combinedData: DataFrame = userData.join(cityData, Seq("userid"), "left_outer").join(jwdData, Seq("userid"), "left_outer")

    import spark.implicits._
    val data: DataFrame = combinedData.rdd.map{row =>
      val userid: String = row.getAs[String]("userid")

      val applist: Seq[String] = if(Option(row.get(1)).isEmpty) Seq() else row.getAs[String](1).split(";").flatMap{appToken =>
        val tokens: Array[String] = appToken.split(",")
        if (tokens.length > 0) Some(tokens(0)) else None
      }

      val stuType: Double = if(Option(row.get(2)).isEmpty) row.getAs[Int]("is_student").toDouble else 0
      val jwd0: Double = if(Option(row.get(3)).isEmpty) row.getAs[Long]("jwd0").toDouble else 0
      val jwd100: Double = if(Option(row.get(4)).isEmpty) row.getAs[Long]("jwd100").toDouble else 0
      val jwd200: Double = if(Option(row.get(5)).isEmpty) row.getAs[Long]("jwd200").toDouble else 0
      val jwd500: Double = if(Option(row.get(6)).isEmpty) row.getAs[Long]("jwd500").toDouble else 0
      val jwd1000: Double = if(Option(row.get(7)).isEmpty) row.getAs[Long]("jwd1000").toDouble else 0
      val jwd0days: Double = if(Option(row.get(8)).isEmpty) row.getAs[Long]("jwd0days").toDouble else 0
      val jwd100days: Double = if(Option(row.get(9)).isEmpty) row.getAs[Long]("jwd100days").toDouble else 0
      val jwd200days: Double = if(Option(row.get(10)).isEmpty) row.getAs[Long]("jwd200days").toDouble else 0
      val jwd500days: Double = if(Option(row.get(11)).isEmpty) row.getAs[Long]("jwd500days").toDouble else 0
      val jwd1000days: Double = if(Option(row.get(12)).isEmpty) row.getAs[Long]("jwd1000days").toDouble else 0

      val jwdVec: linalg.Vector = Vectors.dense(Array(jwd0, jwd100, jwd200, jwd500, jwd1000, jwd0days, jwd100days, jwd200days, jwd500days, jwd1000days))
      (userid, applist, stuType, jwdVec)
    }.toDF("userid", "applist", "stuType", "jwdVec")

    data
  }
}
