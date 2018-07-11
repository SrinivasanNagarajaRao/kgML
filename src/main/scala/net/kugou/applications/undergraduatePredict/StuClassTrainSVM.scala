package net.kugou.applications.undergraduatePredict

import net.kugou.algorithms.MultiClassifications
import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, VectorAssembler}
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Author: yhao
  * Time: 2018/06/12 9:55
  * Desc: 
  *
  */
object StuClassTrainSVM extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val trainData: DataFrame = loadTrainData(spark)
    val testData: DataFrame = loadTestData(spark)

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("applist")
      .setOutputCol("app_vec")
      .setVocabSize(10000)
      .fit(trainData)
    val transTrainData: DataFrame = cvModel.transform(trainData)
    val transTestData: DataFrame = cvModel.transform(testData)

    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("app_vec", "stuType", "jwdVec"))
      .setOutputCol("features")
    val vecTrainData: DataFrame = assembler.transform(transTrainData)
    val vecTestData: DataFrame = assembler.transform(transTestData)

//    val Array(trainData, testData) = vecData.randomSplit(Array(0.7, 0.3), seed = 1234L)

    val model: NaiveBayesModel = new NaiveBayes().setLabelCol("label").setFeaturesCol("features").fit(vecTrainData)
//    val model: LogisticRegressionModel = new LogisticRegression().setLabelCol("label").setFeaturesCol("features").fit(vecTrainData)
//    val model: DecisionTreeClassificationModel = new DecisionTreeClassifier().setLabelCol("label").setFeaturesCol("features").fit(vecTrainData)
//    val model: RandomForestClassificationModel = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features").fit(vecTrainData)
//    val model: GBTClassificationModel = new GBTClassifier().setLabelCol("label").setFeaturesCol("features").fit(vecTrainData)
    val predictions: DataFrame = model.transform(vecTestData)

    val evaluateData: RDD[(Double, Double)] = predictions.select("label", "prediction").rdd.map{row =>
      (row.getDouble(0), row.getDouble(1))
    }

    val mc: MultiClassifications = new MultiClassifications(spark)
    mc.evaluate(evaluateData)

    spark.stop()
  }



  def loadTrainData(spark: SparkSession): DataFrame = {
    val zeroSQL: String = "select userid, label from (" +
      "select userid, case when profession like '%大学生%' then 1 else 0 end label from temp.survey_userid_C_0611_bak" +
      ") tmp where label=0"
    val oneSQL: String = "select userid, 1 label from mllab.stu_appjwdstu_new where type='appjwdstu' limit 10000"
    println(s"Load zero user data sql: \n\t$zeroSQL")
    println(s"Load one user data sql: \n\t$oneSQL")

    val appSQL: String = "select userid, app_list from dsl.dwf_userinfo_digging_3month_d where dt='2018-05-31' and app_list is not null"
    val citySQL: String = "select userid, is_student from temp.stu_result_city_new"
    val jwdSQL: String = "select userid, jwd0, jwd100, jwd200, jwd500, jwd1000, jwd0days, jwd100days, jwd200days, jwd500days, jwd1000days from mllab.stu_result_userid3"
    println(s"Load app data sql: \n\t$appSQL")
    println(s"Load city data sql: \n\t$citySQL")
    println(s"Load jwd data sql: \n\t$jwdSQL")

    val zeroData: DataFrame = spark.sql(zeroSQL)
    val oneData: DataFrame = spark.sql(oneSQL)
    val userData: Dataset[Row] = zeroData.union(oneData)

    val appData: DataFrame = spark.sql(appSQL)
    val cityData: DataFrame = spark.sql(citySQL)
    val jwdData: DataFrame = spark.sql(jwdSQL)

    val combinedData: DataFrame = userData
      .join(appData, "userid")
      .join(cityData, Seq("userid"), "left_outer")
      .join(jwdData, Seq("userid"), "left_outer")

    import spark.implicits._
    val data: DataFrame = combinedData.rdd.map{row =>
      val userid: String = row.getAs[String]("userid")
      val label: Double = row.getInt(1).toDouble

      val applist: Seq[String] = if(Option(row.get(2)).isEmpty) Seq() else row.getAs[String](2).split(";").flatMap{appToken =>
        val tokens: Array[String] = appToken.split(",")
        if (tokens.length > 0) Some(tokens(0)) else None
      }

      val stuType: Double = if(Option(row.get(3)).isEmpty) row.getAs[Int]("is_student").toDouble else 0
      val jwd0: Double = if(Option(row.get(4)).isEmpty) row.getAs[Long]("jwd0").toDouble else 0
      val jwd100: Double = if(Option(row.get(5)).isEmpty) row.getAs[Long]("jwd100").toDouble else 0
      val jwd200: Double = if(Option(row.get(6)).isEmpty) row.getAs[Long]("jwd200").toDouble else 0
      val jwd500: Double = if(Option(row.get(7)).isEmpty) row.getAs[Long]("jwd500").toDouble else 0
      val jwd1000: Double = if(Option(row.get(8)).isEmpty) row.getAs[Long]("jwd1000").toDouble else 0
      val jwd0days: Double = if(Option(row.get(9)).isEmpty) row.getAs[Long]("jwd0days").toDouble else 0
      val jwd100days: Double = if(Option(row.get(10)).isEmpty) row.getAs[Long]("jwd100days").toDouble else 0
      val jwd200days: Double = if(Option(row.get(11)).isEmpty) row.getAs[Long]("jwd200days").toDouble else 0
      val jwd500days: Double = if(Option(row.get(12)).isEmpty) row.getAs[Long]("jwd500days").toDouble else 0
      val jwd1000days: Double = if(Option(row.get(13)).isEmpty) row.getAs[Long]("jwd1000days").toDouble else 0

      val jwdVec: linalg.Vector = Vectors.dense(Array(jwd0, jwd100, jwd200, jwd500, jwd1000, jwd0days, jwd100days, jwd200days, jwd500days, jwd1000days))
      (userid, label, applist, stuType, jwdVec)
    }.toDF("userid", "label", "applist", "stuType", "jwdVec")

    data
  }


  def loadTestData(spark: SparkSession): DataFrame = {
    val userSQL: String = "select t1.userid, case when t1.profession like '%大学生%' then 1 else 0 end label " +
      "from temp.survey_userid_C_0611_ture t1 left outer join temp.survey_userid_C_0611_ture_bak t2 on t1.userid=t2.userid " +
      "where t2.userid is null "

    val appSQL: String = "select userid, app_list from dsl.dwf_userinfo_digging_3month_d where dt='2018-05-31' and app_list is not null"
    val citySQL: String = "select userid, is_student from temp.stu_result_city_new"
    val jwdSQL: String = "select userid, jwd0, jwd100, jwd200, jwd500, jwd1000, jwd0days, jwd100days, jwd200days, jwd500days, jwd1000days from mllab.stu_result_userid3"
    println(s"Load app data sql: \n\t$appSQL")
    println(s"Load city data sql: \n\t$citySQL")
    println(s"Load jwd data sql: \n\t$jwdSQL")

    val userData: DataFrame = spark.sql(userSQL)

    val appData: DataFrame = spark.sql(appSQL)
    val cityData: DataFrame = spark.sql(citySQL)
    val jwdData: DataFrame = spark.sql(jwdSQL)

    val combinedData: DataFrame = userData
      .join(appData, "userid")
      .join(cityData, Seq("userid"), "left_outer")
      .join(jwdData, Seq("userid"), "left_outer")

    import spark.implicits._
    val data: DataFrame = combinedData.rdd.map{row =>
      val userid: String = row.getAs[String]("userid")
      val label: Double = row.getInt(1).toDouble

      val applist: Seq[String] = if(Option(row.get(2)).isEmpty) Seq() else row.getAs[String](2).split(";").flatMap{appToken =>
        val tokens: Array[String] = appToken.split(",")
        if (tokens.length > 0) Some(tokens(0)) else None
      }

      val stuType: Double = if(Option(row.get(3)).isEmpty) row.getAs[Int]("is_student").toDouble else 0
      val jwd0: Double = if(Option(row.get(4)).isEmpty) row.getAs[Long]("jwd0").toDouble else 0
      val jwd100: Double = if(Option(row.get(5)).isEmpty) row.getAs[Long]("jwd100").toDouble else 0
      val jwd200: Double = if(Option(row.get(6)).isEmpty) row.getAs[Long]("jwd200").toDouble else 0
      val jwd500: Double = if(Option(row.get(7)).isEmpty) row.getAs[Long]("jwd500").toDouble else 0
      val jwd1000: Double = if(Option(row.get(8)).isEmpty) row.getAs[Long]("jwd1000").toDouble else 0
      val jwd0days: Double = if(Option(row.get(9)).isEmpty) row.getAs[Long]("jwd0days").toDouble else 0
      val jwd100days: Double = if(Option(row.get(10)).isEmpty) row.getAs[Long]("jwd100days").toDouble else 0
      val jwd200days: Double = if(Option(row.get(11)).isEmpty) row.getAs[Long]("jwd200days").toDouble else 0
      val jwd500days: Double = if(Option(row.get(12)).isEmpty) row.getAs[Long]("jwd500days").toDouble else 0
      val jwd1000days: Double = if(Option(row.get(13)).isEmpty) row.getAs[Long]("jwd1000days").toDouble else 0

      val jwdVec: linalg.Vector = Vectors.dense(Array(jwd0, jwd100, jwd200, jwd500, jwd1000, jwd0days, jwd100days, jwd200days, jwd500days, jwd1000days))
      (userid, label, applist, stuType, jwdVec)
    }.toDF("userid", "label", "applist", "stuType", "jwdVec")

    data
  }
}
