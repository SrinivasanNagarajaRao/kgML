package net.kugou.applications.songRecommend

import net.kugou.utils.SparkUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Rating(userId: Int, movieId: Int, rating: Float)

/**
  * Author: yhao
  * Time: 2018/07/11 14:24
  * Desc: 
  *
  */
object RecommendByCF extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val checkpointPath = "hdfs:/user/haoyan/checkpoint"
    val path = new Path(checkpointPath)
    val sc = spark.sparkContext
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    if(hdfs.exists(path)){
      hdfs.delete(path,true)
      println(s"成功删除 $checkpointPath")
    }
    sc.setCheckpointDir(checkpointPath)

    val data: DataFrame = spark.read.table("temp.tmp_cds_20180707_user_list_02")

    val userData: RDD[(Int, Int, String, Int, Int, Int)] = data.rdd.flatMap { row =>
      val userid: String = if (Option(row.get(0)).nonEmpty) row.getString(0) else ""
      val songStr: String = if (Option(row.get(1)).nonEmpty) row.getString(1) else ""

      if (songStr.nonEmpty && userid.nonEmpty && !userid.contains(".") && userid!='0') {
        val songList: List[String] = songStr.split(",").toList
        if (songList.size > 15) { //资产数小于15的用户直接过滤掉
          songList.flatMap { song =>
            val tokens: Array[String] = song.split("_")
            if (tokens.length>4) {
              val scid: String = tokens(0)
              val date: String = tokens(1)
              val playCnt: Int = tokens(2).toInt
              val play30Cnt: Int = tokens(3).toInt
              val play90Cnt: Int = tokens(4).toInt

              // 过滤未播放的资产
              if (playCnt!=0 || play30Cnt!=0 || play90Cnt!=0) {
                Some((userid.toInt, scid.toInt, date, playCnt, play30Cnt, play90Cnt))
              } else None
            } else None
          }
        } else None
      } else None
    }

    import spark.implicits._
    val userItemRatingData: DataFrame = userData.map{ record =>
      val (userid, scid, date, playCnt, play30Cnt, play90Cnt) = record

      // 评分，未播放歌曲:0; 播放小于30%:-2; 播放小于90%:2; 播放大于90%:5
      // val rating: Double = if (playCnt==0 && play30Cnt==0 && play90Cnt==0) 0.0 else -2.0*playCnt + 2.0*play30Cnt + 5.0*play90Cnt
      val rating: Float = -2.0f*playCnt + 2.0f*play30Cnt + 5.0f*play90Cnt
      Rating(userid, scid, rating)
    }.toDF()

    // 切分训练集和测试集
    val Array(trainData, testData) = userItemRatingData.randomSplit(Array(0.75, 0.25))

    var evaluateList: List[(Double, Double)] = Nil
    for (i <- Range(0, 10)) {
      val regParam: Double = 0.1 * (1 + i)

      val als: ALS = new ALS()
        .setImplicitPrefs(false)
        .setNumBlocks(1000)
        .setRegParam(regParam)
        .setMaxIter(50)
        .setRank(100)
        .setAlpha(1.0)
        .setUserCol("userid")
        .setItemCol("scid")
        .setRatingCol("rating")

      val alsModel: ALSModel = als.fit(trainData)
      val predictions: DataFrame = alsModel.transform(testData)

      // 评估预测效果，使用RMSE
      val evaluator: RegressionEvaluator = new RegressionEvaluator()
        .setMetricName("rmse")
        .setLabelCol("rating")
        .setPredictionCol("prediction")
      val rmse = evaluator.evaluate(predictions)

      println("regParam: " + regParam + ", RMSE: " + rmse)
      evaluateList :+= (regParam, rmse)
    }

    println("======regParam")
    evaluateList.foreach(println)
    spark.stop()
  }


  def parseData(spark: SparkSession, tableName: String): DataFrame = {
    null
  }
}
