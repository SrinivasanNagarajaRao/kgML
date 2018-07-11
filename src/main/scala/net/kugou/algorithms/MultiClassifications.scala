package net.kugou.algorithms

import org.apache.log4j.Logger
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{ml, mllib}


/**
  * 多分类训练与测试工具类
  *
  * @param spark    SparkSession
  * @author   yhao
  */
class MultiClassifications(val spark: SparkSession) extends Serializable {
  val logger: Logger = Logger.getLogger(this.getClass)

  private var method: String = "LR"             //分类器

  //============================ LR参数
  private var iterNum: Int = 100                //最大迭代次数
  private var regParam: Double = 0.01           //正则化项系数
  private var aggregationDepth: Int = 2         //聚合深度，建议不小于2，如果特征较多，或者分区较多时可适当增大该值
  private var elasticNetParam: Double = 0.0     //LR弹性网络参数
  private var tol: Double = 1E-6                //误差阈值

  //============================ SVM参数
  private var stepSize: Double = 1.0            //迭代步长
  private var minBatchFraction: Double = 1.0    //最小批次大小

  //============================ RF参数
  private var maxDepth: Int = 5                 //树的最大深度 Maximum depth of the tree (>= 0). E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes. (default = 5)
  private var numTrees: Int = 20                //树个数 Number of trees to train (>= 1). If 1, then no bootstrapping is used.  If > 1, then bootstrapping is done. (default = 20)
  private var maxBins: Int = 32                 //用于将连续型特征离散化的最大容器数 Maximum number of bins used for discretizing continuous features and for choosing how to split on features at each node.  More bins give higher granularity. Must be >= 2 and >= number of categories in any categorical feature. (default = 32)
  private var featureSubsetStrategy: String = "auto"              //树节点分裂时涉及的特征数 The number of features to consider for splits at each tree node.
  private var minInstancePerNode: Int = 1       //分割后每个子节点的最小实例数 Minimum number of instances each child must have after split. If a split causes the left or right child to have fewer than minInstancesPerNode, the split will be discarded as invalid. Should be >= 1. (default = 1)
  private var minInfoGain: Double = 0.0         //特征选择时最小信息增益 Minimum information gain for a split to be considered at a tree node. Should be >= 0.0. (default = 0.0)
  private var impurity: String = "gini"         //信息增益计算规则，支持："entropy" 或 "gini" Criterion used for information gain calculation (case-insensitive). Supported: "entropy" and "gini". (default = gini)
  private var subsamplingRate: Double = 1.0     //每棵子树使用训练集比例 Fraction of the training data used for learning each decision tree, in range (0, 1]. (default = 1.0)
  private var seed: Long = this.getClass.getName.hashCode.toLong  //随机数种子


  private var featureCol: String = "features"
  private var labelCol: String = "label"
  private var predictionCol: String = "prediction"


  def setMethod(value: String): this.type = {
    require(value.equalsIgnoreCase("LR") || value.equalsIgnoreCase("SVM") || value.equalsIgnoreCase("RF"), "目前支持的分类算法：LR/SVM/RF")
    this.method = value
    this
  }

  def setIterNum(value: Int): this.type = {
    this.iterNum = value
    this
  }

  def setRegParam(value: Double): this.type = {
    this.regParam = value
    this
  }

  def setAggregationDepth(value: Int): this.type = {
    this.aggregationDepth = value
    this
  }

  def setElasticNetParam(value: Double): this.type = {
    this.elasticNetParam = value
    this
  }

  def setTol(value: Double): this.type = {
    this.tol = value
    this
  }

  def setStepSize(value: Double): this.type = {
    this.stepSize = value
    this
  }

  def setMinBatchFraction(value: Double): this.type = {
    this.minBatchFraction = value
    this
  }

  def setMaxDepth(value: Int): this.type = {
    require(value > 0, "树最大深度必须大于0")
    this.maxDepth = value
    this
  }

  def setNumTrees(value: Int): this.type = {
    require(value >= 1, "树的数量必须大于0，如果为1，则不使用提升算法")
    this.numTrees = value
    this
  }

  def setMaxBins(value: Int): this.type = {
    this.maxBins = value
    this
  }

  def setFeatureSubsetStrategy(value: String): this.type = {
    this.featureSubsetStrategy = value
    this
  }

  def setMinInstancePerNode(value: Int): this.type = {
    require(value >= 1, "每个子节点实例数必须大于0")
    this.minInstancePerNode = value
    this
  }

  def setMinInfoGain(value: Double): this.type = {
    this.minInfoGain = value
    this
  }

  def setImpurity(value: String): this.type = {
    require(value.equalsIgnoreCase("entropy") || value.equalsIgnoreCase("gini"), "信息增益计算只支持\"entropy\"或\"gini\"")
    this.impurity = value
    this
  }

  def setSubsamplingRate(value: Double): this.type = {
    require(value > 0.0 && value <= 1.0, "子树使用特征数比例必须在(0, 1]")
    this.subsamplingRate = value
    this
  }

  def setSeed(value: Long): this.type = {
    this.seed = value
    this
  }

  def setFeatureCol(value: String): this.type = {
    this.featureCol = value
    this
  }

  def setLabelCol(value: String): this.type = {
    this.labelCol = value
    this
  }

  def setPredictionCol(value: String): this.type = {
    this.predictionCol = value
    this
  }


  def train(data: DataFrame) = {
    val model = method match {
      case "LR" => trainWithLR(data)
      case _ =>
    }

    model
  }


  /**
    * 使用LR模型进行训练
    *
    * @param data   待训练数据
    * @return   LR模型
    */
  def trainWithLR(data: DataFrame): LogisticRegressionModel = {
    val lr: LogisticRegression = new LogisticRegression()
      .setFeaturesCol(featureCol)
      .setLabelCol(labelCol)
      .setPredictionCol(predictionCol)
      .setMaxIter(iterNum)
      .setRegParam(regParam)
      .setAggregationDepth(aggregationDepth)
      .setElasticNetParam(elasticNetParam)
      .setTol(tol)

    println("\n模型参数：")
    println(s"\t最大迭代次数：$iterNum")
    println(s"\t正则化参数：$regParam")
    println(s"\t聚合深度：$aggregationDepth")
    println(s"\tL1:L2正则比例：$elasticNetParam")
    println(s"\t收敛阈值：$tol")

    println("开始训练模型...")
    val startTime: Long = System.currentTimeMillis()
    val model: LogisticRegressionModel = lr.fit(data)
    val totalTime: Double = (System.currentTimeMillis() - startTime) / 1e3
    println(s"模型训练完成！耗时：$totalTime sec")

    model
  }


  def trainWithSVM(data: DataFrame): SVMModel = {
    val trainData: RDD[LabeledPoint] = data.select(labelCol, featureCol).rdd.map{row =>
      LabeledPoint(row.getDouble(0), mllib.linalg.Vectors.dense(row.getAs[ml.linalg.Vector](1).toArray))
    }

    println("\n模型参数：")
    println(s"\t最大迭代次数：$iterNum")
    println(s"\t正则化参数：$regParam")
    println(s"\t迭代步长：$stepSize")
    println(s"\t最小批次大小：$minBatchFraction")

    println("开始训练模型...")
    val startTime: Long = System.currentTimeMillis()
    val model: SVMModel = SVMWithSGD.train(trainData, iterNum, stepSize, regParam, minBatchFraction)
    val totalTime: Double = (System.currentTimeMillis() - startTime) / 1e3
    println(s"模型训练完成！耗时：$totalTime sec")

    model.clearThreshold()

    model
  }


  def trainWithRF(data: DataFrame): RandomForestClassificationModel = {
    val rf: RandomForestClassifier = new RandomForestClassifier()
      .setFeaturesCol(featureCol)
      .setLabelCol(labelCol)
      .setPredictionCol(predictionCol)
      .setNumTrees(numTrees)
      .setMaxDepth(maxDepth)
      .setMaxBins(maxBins)
      .setMinInstancesPerNode(minInstancePerNode)
      .setMinInfoGain(minInfoGain)
      .setImpurity(impurity)
      .setSubsamplingRate(subsamplingRate)
      .setFeatureSubsetStrategy(featureSubsetStrategy)
      .setSeed(seed)

    println("\n模型参数：")
    println(s"\t决策树数目：$numTrees")
    println(s"\t数最大深度：$maxDepth")
    println(s"\t离散化容器最大个数：$maxBins")
    println(s"\t每个节点实例数：$minInstancePerNode")
    println(s"\t最小信息增益：$minInfoGain")
    println(s"\t特征选择规则：$impurity")
    println(s"\t子树使用训练集比例：$subsamplingRate")
    println(s"\t随机种子：$seed")

    println("开始训练模型...")
    val startTime: Long = System.currentTimeMillis()
    val model: RandomForestClassificationModel = rf.fit(data)
    val totalTime: Double = (System.currentTimeMillis() - startTime) / 1e3
    println(s"模型训练完成！耗时：$totalTime sec")

    model
  }


  /**
    * 使用LR模型进行预测
    *
    * @param data     待测试数据
    * @param model    LR模型
    * @return   预测结果
    */
  def predictWithLR(data: DataFrame, model: LogisticRegressionModel): DataFrame = {
    println("\n开始预测数据...")
    val startTime: Long = System.currentTimeMillis()
    val predictions: DataFrame = model.transform(data)
    val totalTime: Double = (System.currentTimeMillis() - startTime) / 1e3
    println(s"预测数据完成！耗时：$totalTime sec")

    predictions
  }


  def predictWithSVM(data: DataFrame, model: SVMModel): DataFrame = {
    println("\n开始预测数据...")
    val startTime: Long = System.currentTimeMillis()
    val predictData: RDD[mllib.linalg.Vector] = data.select(featureCol).rdd.map(row => mllib.linalg.Vectors.dense(row.getAs[ml.linalg.Vector](0).toArray))

    import spark.implicits._
    val tmpPredictions: DataFrame = model.predict(predictData).toDF(predictionCol)
    val predictions: DataFrame = data.withColumn(predictionCol, tmpPredictions.col(predictionCol))
    val totalTime: Double = (System.currentTimeMillis() - startTime) / 1e3
    println(s"预测数据完成！耗时：$totalTime sec")

    predictions
  }


  def predictWithRF(data: DataFrame, model: RandomForestClassificationModel): DataFrame = {
    println("\n开始预测数据...")
    val startTime: Long = System.currentTimeMillis()
    val predictions: DataFrame = model.transform(data)
    val totalTime: Double = (System.currentTimeMillis() - startTime) / 1e3
    println(s"预测数据完成！耗时：$totalTime sec")

    predictions
  }


  /**
    * LR模型预测结果评估
    *
    * @param LabelsAndprediction    LR模型预测结果，包括: (标签/预测结果)
    * @return   (准确率, (F1值, 精确率, 召回率))
    */
  def evaluate(LabelsAndprediction: RDD[(Double, Double)]): (Double, (Double, Double, Double)) = {
    println("\n开始进行模型评估...")

    //打印每个类别下的查准率
    println("\n每个类别下的详细查准率：")
    LabelsAndprediction.map(_.swap).groupByKey().map { record =>
      var tmpCount: Int = 0
      val count: Int = record._2.size
      record._2.foreach{pred => if(pred == record._1) tmpCount += 1}
      (record._1, tmpCount, count)
    }.sortBy(_._1).collect().foreach { record =>
      println(s"类别：${record._1}, 查准率：${(1.0 * record._2 / record._3) * 100}%, 预测准确数：${record._2}, 预测总数：${record._3}")
    }

    //打印每个类别下的查全率
    println("\n每个类别下的详细查全率：")
    LabelsAndprediction.groupByKey().map { record =>
      var tmpCount: Int = 0
      val count: Int = record._2.size
      record._2.foreach{pred => if(pred == record._1) tmpCount += 1}
      (record._1, tmpCount, count)
    }.sortBy(_._1).collect().foreach { record =>
      println(s"类别：${record._1}, 查全率：${(1.0 * record._2 / record._3) * 100}%, 预测准确数：${record._2}, 真实总数：${record._3}")
    }

    val metrics = new MulticlassMetrics(LabelsAndprediction)

    val f1: Double = metrics.weightedFMeasure
    val weightedPrecision: Double = metrics.weightedPrecision
    val weightedRecall: Double = metrics.weightedRecall
    val accuracy: Double = metrics.accuracy

    println("模型评估结果：")
    println(s"\t准确率：$accuracy")
    println(s"\t查准率：$weightedPrecision")
    println(s"\t查全率：$weightedRecall")
    println(s"\tF1-Measure：$f1")

    (accuracy, (f1, weightedPrecision, weightedRecall))
  }
}
