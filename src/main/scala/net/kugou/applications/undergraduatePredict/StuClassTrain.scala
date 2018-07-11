package net.kugou.applications.undergraduatePredict

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Author: yhao
  * Time: 2018/06/12 9:55
  * Desc: 
  *
  */
object StuClassTrain extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val modelPath: String = "hdfs://kgdc/user/haoyan/models/college_class_dm/"

    //加载数据
    val data: DataFrame = loadTrainData(spark)

    //文本特征向量化
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("applist")
      .setOutputCol("app_vec")
      .setVocabSize(20000)
      .fit(data)
    cvModel.write.overwrite().save(modelPath + "cvModel")
    val vecData: DataFrame = cvModel.transform(data)

    val idfModel: IDFModel = new IDF()
      .setInputCol("app_vec")
      .setOutputCol("app_idf")
      .fit(vecData)
    idfModel.write.overwrite().save(modelPath + "idfModel")
    val idfData: DataFrame = idfModel.transform(vecData)

    //聚合特征列
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("app_idf", "stuType", "jwdVec"))
      .setOutputCol("features")
    val assembleData: DataFrame = assembler.transform(idfData)

    val model: NaiveBayesModel = new NaiveBayes()
      .setLabelCol("label")
      .setFeaturesCol("features")
//      .setWeightCol("weight")
      .fit(assembleData)
    model.write.overwrite().save(modelPath + "nbModel")

    spark.stop()
  }


  def loadTrainData(spark: SparkSession): DataFrame = {
    val zeroSQL: String = "select userid, 0 label from mllab.userid_predict_for_train " +
      "where prediction=0.0 and abs(prob_0-prob_1)>0.8 limit 500000"
    val oneSQL: String = "select userid, 1 label from mllab.stu_4_appjwdstu where type='appjwdstu'"

    val appSQL: String = "select userid, app_list from dsl.dwf_userinfo_digging_3month_d where dt='2018-05-31' and app_list is not null"
    val citySQL: String = "select userid, is_student from temp.stu_result_city_new"
    val jwdSQL: String = "select userid, jwd0, jwd100, jwd200, jwd500, jwd1000, jwd0days, jwd100days, jwd200days, jwd500days, jwd1000days from mllab.stu_result_userid3"
    println(s"Load app data sql: \n\t$appSQL")
    println(s"Load city data sql: \n\t$citySQL")
    println(s"Load jwd data sql: \n\t$jwdSQL")

    val zeroData: DataFrame = spark.sql(zeroSQL)
    val oneData: DataFrame = spark.sql(oneSQL)
    val zero_cnt: Long = zeroData.count()
    val one_cnt: Long = oneData.count()

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

      val weight: Double = if (label > 0) 1.0 * zero_cnt / (zero_cnt + one_cnt) else 1.0 * one_cnt / (zero_cnt + one_cnt)

      val jwdVec: linalg.Vector = Vectors.dense(Array(jwd0, jwd100, jwd200, jwd500, jwd1000, jwd0days, jwd100days, jwd200days, jwd500days, jwd1000days))
      (userid, label, applist, stuType, jwdVec, weight)
    }.toDF("userid", "label", "applist", "stuType", "jwdVec", "weight")

    data
  }
}
