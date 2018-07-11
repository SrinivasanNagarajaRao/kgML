package net.kugou.applications.undergraduatePredict

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: yhao
  * Time: 2018/06/12 9:55
  * Desc: 
  *
  */
object StuClassTrainPre extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val modelPath: String = "hdfs://kgdc/user/haoyan/models/college/test/"

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

    //聚合特征列
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("app_vec", "stuType", "jwdVec"))
      .setOutputCol("features")
    val assembleData: DataFrame = assembler.transform(vecData)

    //训练并保存NB模型
    val nbModel: NaiveBayesModel = new NaiveBayes()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .fit(assembleData)
    nbModel.write.overwrite().save(modelPath + "nbModel")

    println("\n============ NB ===============")
    println(s"Thresholds: ${nbModel.thresholds}")

    //训练并保存LR模型
    val lrModel: LogisticRegressionModel = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .fit(assembleData)
    lrModel.write.overwrite().save(modelPath + "lrModel")

    println("\n============ LR ===============")
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    //训练并保存DT模型
    val dtModel: DecisionTreeClassificationModel = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .fit(assembleData)
    dtModel.write.overwrite().save(modelPath + "dtModel")

    println("\n============ DT ===============")
    println(s"Thresholds: ${dtModel.thresholds}")

    //训练并保存RF模型
    val rfModel: RandomForestClassificationModel = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .fit(assembleData)
    rfModel.write.overwrite().save(modelPath + "rfModel")

    println("\n============ RF ===============")
    println(s"Thresholds: ${rfModel.thresholds}")

    //训练并保存GBT模型
    val gbtModel: GBTClassificationModel = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .fit(assembleData)
    gbtModel.write.overwrite().save(modelPath + "gbtModel")

    spark.stop()
  }


  def loadTrainData(spark: SparkSession): DataFrame = {
    null
  }
}
