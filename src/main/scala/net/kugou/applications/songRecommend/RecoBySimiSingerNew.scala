package net.kugou.applications.songRecommend

import net.kugou.utils.SparkUtils
import net.kugou.utils.distance.Distances
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.{OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

/**
  * Author: yhao
  * Time: 2018/07/31 11:46
  * Desc: 相似歌手歌曲推荐，根据用户资产歌曲找到topN歌手，找对对应的相似歌手，计算原歌手对应歌曲特征向量以及相似歌手歌曲特征向量，
  *       如果推荐歌曲与对应歌曲中50%以上相似度达到80%，则保留该推荐歌曲
  *
  */
object RecoBySimiSingerNew extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val checkpointPath = "hdfs:/user/haoyan/checkpoint"
    val spark: SparkSession = SparkUtils().createSparkEnv(checkPointDir = checkpointPath)

    val scidFeatureTable: String = "mllab.scid_character_total_d_new"           // 歌曲特征表
    val scidLoudnessTable: String = "mllab.scid_character_total_01_d_new"       // 歌曲响度表
    val ratingTable: String = "temp.tb_user_item_rating_500w"                    // 用户歌曲评分表
    val singerCompaTable: String = "mllab.t_new_song_d_pool_allinfo"            // 歌手歌曲关联表
    val simiSingerTable: String = "temp.wjh_singerid_similar_20180722"          // 相似歌手表
    val newSongTable: String = "mllab.t_new_song_provided_scid"                 // 新歌表
    val tmpResultTable: String = "mllab.tb_user_item_bySinger_500w_result_new"   // 临时结果表
    val resultTable: String = "mllab.tb_simi_singer_recommend_500w_result_new"       // 结果表
    val dt: String = "2018-07-24"

    // 合并歌曲特征和响度、音调，并对部分特征进行OneHot编码
    val scidFeatureSQL: String = s"select cast(scid as string) scid, language, bpm, genre, rhythm1, rhythm2, rhythm3 from $scidFeatureTable where dt='$dt'"
    val scidLoudnessSQL: String = s"select scid, loudness_label, keydetect_label from $scidLoudnessTable where dt='$dt'"
    val scidFeatureData: DataFrame = spark.sql(scidFeatureSQL)
    val scidLoudnessData: DataFrame = spark.sql(scidLoudnessSQL)
    val assembledScidData: DataFrame = assembleScidFeatures(scidFeatureData, scidLoudnessData)

    // 加载用户歌曲评分数据，并合并歌曲特征数据以及歌曲对应歌手
    val ratingSQL: String = s"select cast(userid as string) userid, scid, rating from $ratingTable"
    val scidSingerSQL: String = s"select scid, singerid from $singerCompaTable where dt='1900-01-01' and singerid <>'' and singerid <> '0'"
    val ratingData: DataFrame = spark.sql(ratingSQL)
    val singerData: DataFrame = spark.sql(scidSingerSQL)
    val userScidData: DataFrame = assembledScidData.join(ratingData, "scid").join(singerData, "scid").select("userid", "scid", "singerid", "rating", "features")

    // 获取用户topN歌手
    val topSingerData: DataFrame = getTopSinger(userScidData, 20)

    // 用户每个歌手关联得到N个相似歌手
    val simiSingerSQL: String = s"select singerid, similarsingerid simiSingerList from $simiSingerTable where singerid <>'' and singerid <> '0'"
    val simisingerData: DataFrame = spark.sql(simiSingerSQL)
    val topWithsimiSingerData: DataFrame = simisingerData.join(topSingerData, "singerid").select("userid", "singerid", "index", "simiSingerList", "scidFeatureList")
    val userSimiSingerData: RDD[(String, String, List[(String, Vector)], Int, String, Double)] = getSimiSinger(topWithsimiSingerData)

    //过滤新歌，生成歌手-歌曲map
    val newSongSQL: String = s"select scid from $newSongTable"
    val newSongData: DataFrame = spark.sql(newSongSQL)
    val singerScidMap: Map[String, List[(String, Vector)]] = singerData.join(newSongData, "scid").join(assembledScidData, "scid")
      .select("scid", "singerid", "features").rdd.map{row =>
      val scid: String = row.getString(0)
      val singerid: String = row.getString(1)
      val scidFeatures: Vector = row.getAs[Vector](2)
      (singerid, List((scid, scidFeatures)))
    }.reduceByKey(_ ::: _).collect().toMap
    val singerScidMapBC: Broadcast[Map[String, List[(String, Vector)]]] = spark.sparkContext.broadcast(singerScidMap)

    // topN相似歌手的歌曲按与原歌手资产歌曲最高相似度大于0.8且歌曲数大于0.5，则保留该歌曲
    val recommendData: DataFrame = recommendSimiScids(spark, userSimiSingerData, singerScidMapBC, 6)

    // 保存临时结果
    recommendData.createOrReplaceTempView("tmpTable")
    spark.sql("set hive.mapred.mode=nonstrict")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    spark.sql(s"create table if not exists $tmpResultTable( " +
      s"userid      string COMMENT 'userid', " +
      s"scidlist    string COMMENT '推荐歌曲列表'" +
      s")partitioned by (dt string) " +
      s"ROW FORMAT DELIMITED " +
      s"FIELDS TERMINATED BY '|' " +
      s"STORED AS TEXTFILE")

    spark.sql(s"INSERT INTO $tmpResultTable PARTITION(dt='$dt') select userid, scidlist from tmpTable")

    // 保存最终结果
    import spark.implicits._
    val result: DataFrame = recommendData.map{row =>
      val userid: String = row.getString(0)
      val tokens: Array[String] = row.getString(1).split(";")
      val scidList: Array[String] = tokens.map{item =>
        val segs: Array[String] = item.split("_")
        (segs(2), segs(3).toDouble)
      }.sortBy(-_._2).map(_._1)
      (userid, scidList.mkString(","), scidList.length)
    }.toDF("userid", "scidlist", "count")

    result.createOrReplaceTempView("resultTable")

    spark.sql(s"create table if not exists $resultTable( " +
      "userid string COMMENT 'userid', " +
      "scidlist string COMMENT '推荐scid列表', " +
      "count int COMMENT '推荐歌曲数'" +
      s")partitioned by (dt string) " +
      s"ROW FORMAT DELIMITED " +
      s"FIELDS TERMINATED BY '|' " +
      s"STORED AS TEXTFILE")

    spark.sql(s"INSERT INTO $resultTable PARTITION(dt='$dt') select userid, scidlist, count from resultTable")

    spark.stop()
  }


  /**
    * 聚合歌曲特征（语种、bpm、流派、节奏）和歌曲响度（响度、音调），并对：语种、bpm、流派、响度、音调 进行One-Hot编码
    *
    * @param scidFeatureData    歌曲特征
    * @param scidLoudnessData   歌曲响度/音调
    * @return
    */
  def assembleScidFeatures(scidFeatureData: DataFrame, scidLoudnessData: DataFrame): DataFrame = {
    val spark: SparkSession = scidFeatureData.sparkSession

    import spark.implicits._
    val scidFeatures: DataFrame = scidFeatureData.join(scidLoudnessData, "scid").map{row =>
      val scid: String = row.getAs[String]("scid")
      var language: Int = row.getAs[Byte]("language").toInt
      var bpm: Int = row.getAs[Byte]("bpm").toInt
      var genre: Int = row.getAs[Byte]("genre").toInt
      var loudness: Int = row.getAs[Byte]("loudness_label").toInt
      var keydetect: Int = row.getAs[Byte]("keydetect_label").toInt
      val rhythm1: Double = row.getAs[Double]("rhythm1")
      val rhythm2: Double = row.getAs[Double]("rhythm2")
      val rhythm3: Double = row.getAs[Double]("rhythm3")

      language = if(language<0) 0 else language
      bpm = if(bpm<0) 0 else bpm
      genre = if(genre<0) 0 else genre
      loudness = if(loudness<0) 0 else loudness
      keydetect = if(keydetect<0) 0 else keydetect

      val rhythm: Vector = Vectors.dense(Array(rhythm1, rhythm2, rhythm3))
      (scid, language, bpm, genre, loudness, keydetect, rhythm)
    }.toDF("scid", "language", "bpm", "genre", "loudness", "keydetect", "rhythm_vec")

    //对指定特征进行One-Hot编码
    val oneHotCols: List[String] = List("language", "bpm", "genre", "loudness", "keydetect")
    var outputCols: Array[String] = Array()
    var scidEncodeData: DataFrame = scidFeatures
    for(col <- oneHotCols) {
      val outputCol: String = col + "_vec"
      val encoder: OneHotEncoder = new OneHotEncoder()
        .setInputCol(col)
        .setOutputCol(outputCol)
        .setDropLast(false)
      scidEncodeData = encoder.transform(scidEncodeData)
      outputCols :+= outputCol
    }

    val assembleCols: Array[String] = outputCols :+ "rhythm_vec"
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(assembleCols)
      .setOutputCol("features")
    val scidAssembleData: DataFrame = assembler.transform(scidEncodeData).select("scid", "features")

    scidAssembleData
  }


  /**
    * 获取用户资产中歌曲评分最高的topN歌手（同一歌手按所有歌曲评分总和）
    *
    * @param data Data
    * @param num  topN
    * @return DataFrame("userid", "singerid", "index", "scidFeatureList")
    */
  def getTopSinger(data: DataFrame, num: Int = 10): DataFrame = {
    val spark: SparkSession = data.sparkSession
    import spark.implicits._
    data.rdd.map{row =>
      val userid: String = row.getString(0)
      val scid: String = row.getString(1)
      val singerid: String = row.getString(2)
      val rating: Double = row.getDouble(3)
      val scidFeatures: Vector = row.getAs[Vector](4)

      ((userid, singerid), List((scid, rating, scidFeatures)))
    }.reduceByKey(_ ::: _).map{record =>
      val (userid, singerid) = record._1
      val sum: Double = record._2.map(_._2).sum
      val scidFeatureList: List[(String, Vector)] = record._2.map(item => (item._1, item._3))

      (userid, List((singerid, sum, scidFeatureList)))
    }.reduceByKey(_ ::: _, 1000).flatMap{record =>
      val userid: String = record._1
      record._2.sortBy(-_._2).take(num).zipWithIndex.map{item =>
        (userid, item._1._1, item._2, item._1._3)
      }
    }.toDF("userid", "singerid", "index", "scidFeatureList")
  }


  /**
    * 根据用户topN歌手获取相似歌手
    *
    * @param data Data
    * @param maxNum 单一歌手最大相似歌手数
    * @param threshold  选择相似歌手的最小相似度
    * @return
    */
  def getSimiSinger(data: DataFrame, maxNum: Int = 10, threshold: Double = 0.9): RDD[(String, String, List[(String, Vector)], Int, String, Double)] = {
    data.rdd.flatMap { row =>
      val userid: String = row.getString(0)
      val singerid: String = row.getString(1)
      val index: Int = row.getInt(2)
      val simisingers: mutable.WrappedArray[String] = row.getAs[mutable.WrappedArray[String]](3)
      val scidList: List[(String, Vector)] = row.getAs[mutable.WrappedArray[Row]](3).map { row =>
        val scid: String = row.getString(0)
        val scidFeature: Vector = row.getAs[Vector](1)
        (scid, scidFeature)
      }.toList

      // 按排名取对应相似歌手数，最大取maxNum个
      val simiNum: Int = if (maxNum - (index / 2) > 3) maxNum - (index / 2) else 3

      // 相似歌手取top simiNum
      val singerList = simisingers.take(simiNum).toList.flatMap { item =>
        val tokens: Array[String] = item.split("_")

        if (tokens.length > 1) {
          val simisinger: String = tokens(0)
          val rating: Double = tokens(1).toDouble

          if (rating >= threshold) {
            Some((userid, List((singerid, scidList, index, simisinger, rating))))
          } else None
        } else None
      }

      singerList :+ (userid, List((singerid, scidList, index, singerid, 1.0)))
    }.reduceByKey(_ ::: _, 1000).flatMap { record =>
      val userid: String = record._1

      var singerSet: Set[String] = Set()
      var filterList: List[(String, String, List[(String, Vector)], Int, String, Double)] = Nil
      record._2.foreach { item =>
        if (!singerSet.contains(item._4)) {
          filterList :+= (userid, item._1, item._2, item._3, item._4, item._5)
          singerSet += item._4
        }
      }
      filterList
    }
  }


  def recommendSimiScids(
                          spark: SparkSession,
                          data: RDD[(String, String, List[(String, Vector)], Int, String, Double)],
                          singerScidMapBC: Broadcast[Map[String, List[(String, Vector)]]],
                          maxNum: Int = 8): DataFrame = {
    val scidData: RDD[(String, String, String, String, Double)] = data.mapPartitions{iter =>
      val singerScidMap: Map[String, List[(String, Vector)]] = singerScidMapBC.value

      iter.flatMap{ record =>
        val userid: String = record._1
        val origSingerid: String = record._2
        val scidMap: Map[String, Vector] = record._3.toMap
        val scidSet: Set[String] = scidMap.keySet
        val index: Int = record._4
        val simisinger: String = record._5
        val similarity: Double = record._6

        val count: Int = if (maxNum - index / 2 > 3) maxNum - index / 2 else 3

        val tmpScidSet: Set[String] = scidSet
        var recommendScidList: List[(String, Double)] = Nil
        if(singerScidMap.get(simisinger).nonEmpty) {
          singerScidMap(simisinger).foreach{pair =>
            if (!tmpScidSet.contains(pair._1)) {
              val cosineList: Iterable[(String, Double)] = scidMap.values.map{item => (pair._1, Distances.cosine(item.toArray, pair._2.toArray))}
              val filterList: Iterable[(String, Double)] = cosineList.filter(_._2 > 0.8)
              val ratio: Double = 1.0 * filterList.size / cosineList.size
              if (ratio > 0.5) recommendScidList :+= cosineList.maxBy(_._2)
            }
          }
        }

        recommendScidList.sortBy(-_._2).take(count).map(pair => (userid, origSingerid, simisinger, pair._1, similarity * pair._2))
      }
    }

    import spark.implicits._
    scidData.map{record =>
      val scidStr: String = record._2 + "_" + record._3 + "_" + record._4 + "_" + record._5
      (record._1, List(scidStr))
    }.reduceByKey(_ ::: _, 500).map{record =>
      (record._1, record._2.mkString(";"))
    }.toDF("userid", "scidlist")
  }
}
