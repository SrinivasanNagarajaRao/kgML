package net.kugou.applications.songRecommend

import net.kugou.utils.SparkUtils
import net.kugou.utils.distance.Distances
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.{Normalizer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

/**
  * Author: yhao
  * Time: 2018/07/18 14:39
  * Desc: 相似歌手歌曲推荐，根据用户资产歌曲找到topN歌手，找对对应的相似歌手，
  *       然后计算用户特征与相似歌手歌曲特征相似度，推荐与用户特征相似度较高的歌曲
  *
  */
object RecoBySimiSingerOld extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val checkpointPath = "hdfs:/user/haoyan/checkpoint"
    val spark: SparkSession = SparkUtils().createSparkEnv(checkPointDir = checkpointPath)

    val ratingTable: String = args(0)         // 用户歌曲评分表
    val singerCompaTable: String = args(1)    // 歌手歌曲关联表
    val newSongTable: String = args(2)        // 新歌表
    val simiSingerTable: String = args(3)     // 相似歌手表
    val userFeatureTable: String = args(4)    // 用户特征表
    val scidFeatureTable: String = args(5)    // 歌曲特征表
    val scidLoudnessTable: String = args(6)   // 歌曲响度表
    val resultTable: String = args(7)         // 结果表
    val mod: Int = args(8).toInt
    val dt: String = args(9)


//    val ratingTable: String = "temp.tb_user_item_rating_500w"
//    val singerCompaTable: String = "mllab.t_new_song_d_pool_allinfo"
//    val newSongTable: String = "mllab.t_new_song_provided_scid"
//    val simiSingerTable: String = "temp.wjh_singerid_similar_20180722"
//    val userFeatureTable: String = "mllab.user_character_add_0102_new"
//    val scidFeatureTable: String = "mllab.scid_character_total_d_new"
//    val scidLoudnessTable: String = "mllab.scid_character_total_01_d_new"
//    val resultTable: String = "mllab.tb_user_item_bySinger_500w_result"
//    val dt: String = "2018-07-24"


//    val sql: String = s"select cast(userid as string) userid, cast(scid as string) scid, rating from $ratingTable"
    val sql: String = s"select cast(userid as string) userid, cast(scid as string) scid, rating from $ratingTable where userid%10=$mod"
    val scidSQL: String = s"select scid, singerid from $singerCompaTable where dt='1900-01-01' and singerid <>'' and singerid <> '0'"
    val newSongSQL: String = s"select scid from $newSongTable"
    val simiSingerSQL: String = s"select singerid, similarsingerid from $simiSingerTable where singerid <>'' and singerid <> '0'"

    val userFeatureSQL: String = s"select userid, language00, language01, language02, language03, language04, language05, " +
      s"language06, language07, language08, language09, language10, language11, language12, language13, language14, " +
      s"language15, language16, language17, language18, language19, language20, language21, language22, language23, " +
      s"language24, genre00, genre01, genre02, genre03, genre04, genre05, genre06, genre07, genre08, genre09, genre10, " +
      s"genre11, genre12, genre13, genre14, genre15, genre16, genre17, genre18, genre19, genre20, genre21, genre22, " +
      s"genre23, bpm00, bpm01, bpm02, bpm03, rhythm01, rhythm02, rhythm03, loudness_label00, loudness_label01, loudness_label02, " +
      s"loudness_label03, loudness_label04, loudness_label05, keydetect_label00, keydetect_label01, keydetect_label02, " +
      s"keydetect_label03, keydetect_label04, keydetect_label05, keydetect_label06, keydetect_label07, " +
      s"keydetect_label08, keydetect_label09, keydetect_label10, keydetect_label11, keydetect_label12, " +
      s"keydetect_label13, keydetect_label14, keydetect_label15, keydetect_label16, keydetect_label17, " +
      s"keydetect_label18, keydetect_label19, keydetect_label20, keydetect_label21, keydetect_label22, " +
      s"keydetect_label23, keydetect_label24 from $userFeatureTable where dt='$dt'"

    val scidFeatureSQL: String = s"select cast(scid as string), language, bpm, genre, rhythm1, rhythm2, rhythm3 " +
      s"from $scidFeatureTable where dt='$dt'"

    val scidLoudnessSQL: String = s"select scid, loudness_label, keydetect_label from $scidLoudnessTable where dt='$dt'"

    val ratingData: DataFrame = spark.sql(sql)
    val scidData: DataFrame = spark.sql(scidSQL)
    val newSongData: DataFrame = spark.sql(newSongSQL)
    val simisingerData: DataFrame = spark.sql(simiSingerSQL)
    val userFeatureData: DataFrame = spark.sql(userFeatureSQL)
    val scidFeatureData: DataFrame = spark.sql(scidFeatureSQL)
    val scidLoudnessData: DataFrame = spark.sql(scidLoudnessSQL)
    val data: DataFrame = ratingData.join(scidData, Seq("scid"))
    println("原始数据条数：" + data.count())

    //计算每个用户分值最高的N个歌手
    val topSingerData: DataFrame = getTopSinger(data, 20)

    //用户每个歌手关联得到N个相似歌手
    val topWithsimiSingerData: DataFrame = topSingerData.join(simisingerData, "singerid")
    val joinWithSimiSingerData = getSimiSinger(topWithsimiSingerData)
    println("与相似歌手关联后数据条数：" + joinWithSimiSingerData.count())

    val userFeatureDF: DataFrame = assembleUserFeatures(userFeatureData)
    val scidFeatureDF: DataFrame = assembleScidFeatures(scidFeatureData, scidLoudnessData)
    val singerScidMap: Map[String, List[(String, Vector)]] = getNewScidFeaturesMap(scidData, newSongData, scidFeatureDF)
    val singerScidMapBC: Broadcast[Map[String, List[(String, Vector)]]] = spark.sparkContext.broadcast(singerScidMap)

    //评分最高的歌手取N首歌，按歌曲与用户特征余弦值倒序取歌曲
    val result: DataFrame = getScidsForSimiSinger(joinWithSimiSingerData, userFeatureDF, singerScidMapBC)

    import spark.implicits._
    val resultStr = result.rdd.map{case Row(userid: String, origSinger: String, simiSinger: String, scid: String, rating: Double) =>
      val scidStr: String = origSinger + "_" + simiSinger + "_" + scid + "_" + rating
      (userid, List(scidStr))
    }.reduceByKey(_ ::: _, 500).map{record =>
      (record._1, record._2.mkString(";"))
    }.toDF("userid", "scidlist")

    resultStr.createOrReplaceTempView("tmpTable")
    spark.sql("set hive.mapred.mode=nonstrict")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql(s"INSERT INTO $resultTable PARTITION(dt='$dt') select userid, scidlist from tmpTable")
//    spark.sql(s"CREATE TABLE $resultTable AS select userid, scidlist from tmpTable")

    spark.stop()
  }


  def getTopSinger(data: DataFrame, num: Int = 10): DataFrame = {
    val spark: SparkSession = data.sparkSession
    import spark.implicits._
    data.rdd.map{row =>
      val scid: String = row.getString(0)
      val userid: String = row.getString(1)
      val rating: Double = row.getDouble(2)
      val singerid: String = row.getString(3)

      ((userid, singerid), List((scid, rating)))
    }.reduceByKey(_ ::: _).map{record =>
      val (userid, singerid) = record._1
      val sum: Double = record._2.map(_._2).sum

      (userid, List((singerid, sum, record._2.map(_._1))))
    }.reduceByKey(_ ::: _, 1000).flatMap{record =>
      val userid: String = record._1
      record._2.sortBy(-_._2).take(num).zipWithIndex.map{item =>
        (userid, item._1._1, item._2, item._1._3)
      }
    }.toDF("userid", "singerid", "index", "scidlist")
  }


  /**
    * 根据歌手相似表获取用户原始歌手的相似歌手
    * @param data 用户数据
    * @return (userid, origSinger, scidlist, simiNum, simiSinger, similarity)
    */
  def getSimiSinger(data: DataFrame): DataFrame = {
    val spark: SparkSession = data.sparkSession

    import spark.implicits._
    data.rdd.flatMap{row =>
      val singerid: String = row.getString(0)
      val userid: String = row.getString(1)
      val index: Int = row.getInt(2)
      val scidList: List[String] = row.getAs[mutable.WrappedArray[String]](3).toList
      val simisingers: mutable.WrappedArray[String] = row.getAs[mutable.WrappedArray[String]](4)

      val simiNum: Int = if (index==0 || index==1) 8 else if (index==2 || index==3) 7 else if (index==4 || index==5) 6
      else if (index==6 || index==7) 6 else if (index==8 || index==9) 5 else if (index==10 || index==11) 4
      else if (index==12 || index==13) 3 else 2

      // 相似歌手取top simiNum
      val singerList: List[(String, List[(String, List[String], Int, String, Double)])] = simisingers.take(simiNum).toList.flatMap{item =>
        val tokens: Array[String] = item.split("_")

        if(tokens.length > 1){
          val simisinger: String = tokens(0)
          val similarity: Double = tokens(1).toDouble
          Some((userid, List((singerid, scidList, simiNum, simisinger, similarity))))
        } else None
      }

      singerList :+ (userid, List((singerid, scidList, simiNum, singerid, 1.0)))
    }.reduceByKey(_ ::: _, 1000).flatMap{record =>    // 过滤不同歌手对应的相同相似歌手
      val userid: String = record._1
      var singerSet: Set[String] = Set()
      var filterList: List[(String, String, List[String], Int, String, Double)] = Nil
      record._2.foreach{item =>
        if (!singerSet.contains(item._4)) {
          filterList :+= (userid, item._1, item._2, item._3, item._4, item._5)
          singerSet += item._4
        }
      }
      filterList
    }.toDF("userid", "origSinger", "scidlist", "simiNum", "simiSinger", "similarity")
  }


  def getScidsForSimiSinger(
                             data: DataFrame,
                             userFeatureData: DataFrame,
                             singerScidMapBC: Broadcast[Map[String, List[(String, Vector)]]]): DataFrame = {
    val spark: SparkSession = data.sparkSession

    val dataWithFeatures = data.join(userFeatureData, "userid").rdd.map{row =>
      val userid: String = row.getString(0)
      val origSinger: String = row.getString(1)
      val scidList: mutable.WrappedArray[String] = row.getAs[mutable.WrappedArray[String]](2)
      val simiNum: Int = row.getInt(3)
      val simiSinger: String = row.getString(4)
      val similarity: Double = row.getDouble(5)
      val userFeatures: Vector = row.getAs[Vector](6)
      (userid, origSinger, scidList, simiNum, simiSinger, similarity, userFeatures)
    }

    import spark.implicits._
    val scidData: DataFrame = dataWithFeatures.mapPartitions{iter =>
      val singerScidMap: Map[String, List[(String, Vector)]] = singerScidMapBC.value
      iter.flatMap{ record =>
        val userid: String = record._1
        val origSinger: String = record._2
        val scidSet: Set[String] = record._3.toSet
        val simiNum: Int = record._4
        val simisinger: String = record._5
        val similarity: Double = record._6
        val userFeatures: Array[Double] = record._7.toArray

        val singerscidList: List[(String, Double)] = if(singerScidMap.get(simisinger).nonEmpty) {
          if (origSinger.equals(simisinger)) {
            singerScidMap(simisinger).flatMap{item =>
              if (!scidSet.contains(item._1)) {
                Some((item._1, similarity * Distances.cosine(userFeatures, item._2.toArray)))
              } else None
            }
          } else singerScidMap(simisinger).map{item =>
            (item._1, similarity * Distances.cosine(userFeatures, item._2.toArray))
          }
        } else Nil

        //歌手歌曲总数
        if(singerscidList.nonEmpty) {
          val count: Int = if (simiNum==8 || simiNum==7) 5 else if (simiNum==6 || simiNum==5) 4 else if (simiNum==4 || simiNum==3) 3 else 2
          singerscidList.sortBy(-_._2).take(count).map(item => (userid, origSinger, simisinger, item._1, item._2))
        } else None
      }
    }.toDF("userid", "origSinger", "simiSinger", "scid", "rating")

    scidData
  }


  def assembleUserFeatures(userFeatureData: DataFrame): DataFrame = {
    val spark: SparkSession = userFeatureData.sparkSession

    import spark.implicits._
    val userEncodeData: DataFrame = userFeatureData.map{row =>
      val userid: String = row.getString(0)
      val languageVec: Vector = Vectors.dense(Array(
        row.getDouble(1), row.getDouble(2), row.getDouble(3), row.getDouble(4), row.getDouble(5), row.getDouble(6),
        row.getDouble(7), row.getDouble(8), row.getDouble(9), row.getDouble(10), row.getDouble(11), row.getDouble(12),
        row.getDouble(13), row.getDouble(14), row.getDouble(15), row.getDouble(16), row.getDouble(17),
        row.getDouble(18), row.getDouble(19), row.getDouble(20), row.getDouble(21), row.getDouble(22),
        row.getDouble(23), row.getDouble(24), row.getDouble(25))
      )

      val genreVec: Vector = Vectors.dense(Array(
        row.getDouble(26), row.getDouble(27), row.getDouble(28), row.getDouble(29),
        row.getDouble(30), row.getDouble(31), row.getDouble(32), row.getDouble(33), row.getDouble(34),
        row.getDouble(35), row.getDouble(36), row.getDouble(37), row.getDouble(38), row.getDouble(39),
        row.getDouble(40), row.getDouble(41), row.getDouble(42), row.getDouble(43), row.getDouble(44),
        row.getDouble(45), row.getDouble(46), row.getDouble(47), row.getDouble(48), row.getDouble(49))
      )

      val bpmVec: Vector = Vectors.dense(Array(row.getDouble(50), row.getDouble(51), row.getDouble(52), row.getDouble(53)))
      val rhythmVec: Vector = Vectors.dense(Array(row.getDouble(54), row.getDouble(55), row.getDouble(56)))
      val loudnessVec: Vector = Vectors.dense(Array(row.getDouble(57), row.getDouble(58), row.getDouble(59), row.getDouble(60), row.getDouble(61), row.getDouble(62)))

      val keydetectVec: Vector = Vectors.dense(Array(
        row.getDouble(63), row.getDouble(64), row.getDouble(65), row.getDouble(66), row.getDouble(67),
        row.getDouble(68), row.getDouble(69), row.getDouble(70), row.getDouble(71), row.getDouble(72),
        row.getDouble(73), row.getDouble(74), row.getDouble(75), row.getDouble(76), row.getDouble(77),
        row.getDouble(78), row.getDouble(79), row.getDouble(80), row.getDouble(81), row.getDouble(82),
        row.getDouble(83), row.getDouble(84), row.getDouble(85), row.getDouble(86), row.getDouble(87))
      )

      (userid, languageVec, genreVec, bpmVec, rhythmVec, loudnessVec, keydetectVec)
    }.toDF("userid", "language_vec", "genre_vec", "bpm_vec", "rhythm_vec", "loudness_vec", "keydetect_vec")

    // 对指定特征进行归一化
    val normalCols: List[String] = List("language_vec", "bpm_vec", "genre_vec", "loudness_vec", "keydetect_vec", "rhythm_vec")
    var outputCols: Array[String] = Array()
    var userNormalData: DataFrame = userEncodeData
    for(col <- normalCols) {
      val outputCol: String = col + "_nml"
      val normalizer: Normalizer = new Normalizer()
        .setInputCol(col)
        .setOutputCol(outputCol)
        .setP(1)    //按1阶范数进行归一化
      userNormalData = normalizer.transform(userNormalData)
      outputCols :+= outputCol
    }

    val assembleCols: Array[String] = outputCols
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(assembleCols)
      .setOutputCol("user_features")
    val userAssembleData: DataFrame = assembler.transform(userNormalData).select("userid", "user_features")

    userAssembleData
  }


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
      .setOutputCol("scid_features")
    val scidAssembleData: DataFrame = assembler.transform(scidEncodeData).select("scid", "scid_features")

    scidAssembleData
  }


  def getNewScidFeaturesMap(scidData: DataFrame, newSongData: DataFrame, scidFeatureData: DataFrame): Map[String, List[(String, Vector)]] = {
    //过滤新歌，生成歌手-歌曲map
    val singerScidMap: Map[String, List[(String, Vector)]] = scidData
      .join(newSongData, "scid")
      .join(scidFeatureData, "scid").rdd.map{row =>
      val scid: String = row.getAs[String]("scid")
      val singerid: String = row.getAs[String]("singerid")
      val scidFeatureVec: Vector = row.getAs[Vector]("scid_features")
      (singerid, List((scid, scidFeatureVec)))
    }.reduceByKey(_ ::: _).collect().toMap

    singerScidMap
  }
}
