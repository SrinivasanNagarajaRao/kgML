package demo.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession


/**
  * Author: yhao
  * Time: 2018/04/04 15:54
  * Desc: Spark 线性回归模型Demo
  *   决策函数：f(w,b; x)=w·x + b
  *
  */
object LinearRegressionDemo {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    // 加载数据
    val training = spark.read.format("libsvm").load("data/data_spark/mllib/sample_linear_regression_data.txt")

    // 设置线性回归模型参数
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setSolver("l-bfgs")

    // 训练模型
    val lrModel = lr.fit(training)

    // 打印线性回归模型系数向量(w)和截距(b)
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // 模型总结
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")     //打印迭代次数
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")     //暂不清楚是什么东东
    trainingSummary.residuals.show()      //显示最终残差
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")       //打印RMSE均方根误差
    println(s"r2: ${trainingSummary.r2}")       //打印决定系数

    spark.stop()
  }

}
