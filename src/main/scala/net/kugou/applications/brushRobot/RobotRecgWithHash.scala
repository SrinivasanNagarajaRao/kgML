package net.kugou.applications.brushRobot

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{PCA, PCAModel}
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Author: yhao
  * Time: 2018/04/11 11:19
  * Desc: 
  *
  */
object RobotRecgWithHash extends Serializable{
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

//    val date: String = "2018-03-20"
//    val plat: String = "android"
//    val k: Int = 1000
//    val srcTable: String = "dsl.dwf_list_play_d"
//    val tarTable: String = "mllab.tb_yh_robot_recg_kmeans_hash_d"
//    val centerPath: String = "hdfs://kgdc/user/haoyan/result/robot/hash"

    val date: String = args(0)
    val plat: String = args(1)
    val k: Int = args(2).toInt
    val srcTable: String = args(3)
    val tarTable: String = args(4)
    val centerPath: String = args(5)

    val sql: String = s"select user_id, sh, sum(play_pv) play_pv from $srcTable " +
      s"where dt='$date' and pt='$plat' and sty='音频' and play_pv>0 group by user_id, sh"

    val data: DataFrame = spark.sql(sql)
    val hashData: RDD[(String, Long)] = data.select("sh", "play_pv").rdd.map(row => (row.getString(0), row.getLong(1)))

    val vocabulary: Map[String, Int] = getVocab(hashData, 100000)
    vocabulary.toList.sortBy(_._2).take(100).foreach(println)
    val vocabBC: Broadcast[Map[String, Int]] = spark.sparkContext.broadcast(vocabulary)

    import spark.implicits._
    val mapedData = data.rdd.map{row =>
      val userid: String = row.getString(0)
      val hash: String = row.getString(1)
      val pv: Long = row.getLong(2)
      (userid, List((hash, pv)))
    }.reduceByKey(_ ::: _).map{ record =>
      val userid: String = record._1
      var termCounts: Seq[(Int, Double)] = Nil
      record._2.foreach{pair =>
        vocabBC.value.get(pair._1) match {
          case Some(index) => termCounts :+= (index, pair._2.toDouble)
          case None =>
        }
      }

      val str: String = termCounts.map(pair => pair._1 + ":" + pair._2).mkString(",")

      (userid, Vectors.sparse(vocabBC.value.size, termCounts), str)
    }.toDF("userid", "features", "featureStr")

/*
    val pca: PCA = new PCA()
      .setInputCol("features")
      .setOutputCol("dimFeatures")
      .setK(k / 20)
    val pcaModel: PCAModel = pca.fit(mapedData)
    val dimData: DataFrame = pcaModel.transform(mapedData)
*/


    val kMeans: KMeans = new KMeans()
      .setFeaturesCol("features")
      .setMaxIter(200)
      .setK(k)
    val model: KMeansModel = kMeans.fit(mapedData)

    val predictions = model.transform(mapedData)

    // 计算模型的均方误差
    val WSSSE = model.computeCost(mapedData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // 打印聚类中心点，并保存
    /*println("Cluster Centers: ")
    val centers: Array[(Int, linalg.Vector)] = model.clusterCenters.zipWithIndex.map(_.swap)
    val centersRDD: RDD[(Int, linalg.Vector)] = spark.sparkContext.parallelize(centers)
    centers.foreach(println)

    import spark.implicits._
    centersRDD.map{pair =>
      pair._1 + ":" + pair._2.toArray.mkString(",")
    }.toDF("centers").write.mode("overwrite").save(centerPath)*/

    // 打印结果样例
    println("Result Samples: ")
    predictions.select("userid", "prediction", "featureStr").show(20)


    predictions.select("userid", "prediction").rdd.map{row =>
      val clusterIndex: Int = row.getDouble(1).toInt
      (clusterIndex, 1)
    }.reduceByKey(_ + _).sortBy(_._2, ascending = false).take(100).foreach(println)


    val createSQL: String = s"CREATE TABLE IF NOT EXISTS $tarTable(" +
      "userid                     string," +
      "cluster                    int," +
      "featureStr                 string" +
      ")partitioned by (dt string) " +
      "ROW FORMAT DELIMITED " +
      "FIELDS TERMINATED BY '|' " +
      "STORED AS TEXTFILE"

    val insertSQL: String = s"INSERT OVERWRITE TABLE $tarTable partition(dt) " +
      s"select userid, prediction as cluster, featureStr, '$date' as dt from tmpTable"

    // 保存结果到hive表
    predictions.select("userid", "prediction", "featureStr").createOrReplaceTempView("tmpTable")

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql(createSQL)
    spark.sql(insertSQL)

    spark.stop()
  }



  def getVocab(data: RDD[(String, Long)], vocabSize: Int = 1 << 18, minDf: Int = 1): Map[String, Int] = {
    val wordCounts: RDD[(String, Long)] = data.reduceByKey(_ + _).map{pair =>
      (pair._1, (pair._2, 1))
    }.reduceByKey{ case ((wc1, df1), (wc2, df2)) =>
      (wc1 + wc2, df1 + df2)
    }.filter { case (word, (wc, df)) =>
      df >= minDf
    }.map { case (word, (count, dfCount)) =>
      (word, count)
    }.cache()

    val fullVocabSize = wordCounts.count()

    val vocab: Map[String, Int] = wordCounts
      .top(math.min(fullVocabSize, vocabSize).toInt)(Ordering.by(_._2))
      .map(_._1).zipWithIndex.toMap

    vocab
  }
}
