package net.kugou.applications.songRecommend

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Author: yhao
  * Time: 2018/07/13 10:38
  * Desc: 
  *
  */
object RecoByItemCF extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val checkpointPath = "hdfs:/user/haoyan/checkpoint"
    val spark: SparkSession = SparkUtils().createSparkEnv(checkPointDir = checkpointPath)

    val sql: String = "select userid, count, scidlist from temp.tmp_cds_20180707_user_list_filter " +
      "where count>20 and count<800 order by rand(1234) limit 5000"
    val data: DataFrame = GenData.genRatingData(spark, sql)

    val matrix: CoordinateMatrix = parse2Matrix(data)
    val simMatrixData: RDD[MatrixEntry] = standardCosine(matrix)

//    testData.join(trainData, Seq("userid"), "left_outer").join(simMatrixData, Seq(""))


    spark.stop()
  }


  /**
    * 将输入数据转换为矩阵
    * @param data DataFrame
    * @return
    */
  def parse2Matrix(data: DataFrame): CoordinateMatrix = {
    val parsedData: RDD[MatrixEntry] = data.rdd.map { case Row(user: Int, item: Int, rate: Float) =>
        MatrixEntry(user, item, rate.toDouble)
    }
    new CoordinateMatrix(parsedData)
  }


  /**
    * 计算矩阵各列之间的余弦相似度
    * @param matrix 输入矩阵
    * @return
    */
  def standardCosine(matrix: CoordinateMatrix): RDD[MatrixEntry] = {
    val similarity: CoordinateMatrix = matrix.toIndexedRowMatrix().columnSimilarities()
    similarity.entries
  }
}
