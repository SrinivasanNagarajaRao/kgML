package net.kugou.applications.songRecommend

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.util.Random

/**
  * Author: yhao
  * Time: 2018/07/18 14:39
  * Desc: 
  *
  */
object RecoBySimiSinger extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val checkpointPath = "hdfs:/user/haoyan/checkpoint"
    val spark: SparkSession = SparkUtils().createSparkEnv(checkPointDir = checkpointPath)

    val ratingTable: String = args(0)
    val singerCompaTable: String = args(1)
    val newSongTable: String = args(2)
    val simiSingerTable: String = args(3)
    val tmpResultTable: String = args(4)
    val resultTable: String = args(5)
    val mod: Int = args(6).toInt
    val dt: String = args(7)

//    115014057, 891378553, 889868598, 1152685307, 1139559310, 1017404770, 1272958046, 574553698, 985354155
//    val ratingTable: String = "temp.tb_user_item_rating_all"
//    val singerCompaTable: String = "mllab.t_new_song_d_pool_allinfo"
//    val newSongTable: String = "mllab.t_new_song_similar_singer_rec"
//    val simiSingerTable: String = "temp.wjh_singerid_similar_20180722"
//    val tmpResultTable: String = "mllab.tb_user_item_bySinger_all_result"
//    val resultTable: String = "mllab.tb_simi_singer_recommend_result"

    val sql: String = s"select userid, scid, rating from $ratingTable where userid%10=$mod"
    val scidSQL: String = s"select scid, singerid from $singerCompaTable where dt='1900-01-01' and singerid <>'' and singerid <> '0'"
    val newSongSQL: String = s"select scid, num from $newSongTable"
    val simiSingerSQL: String = s"select singerid, similarsingerid from $simiSingerTable where singerid <>'' and singerid <> '0'"

    val ratingData: DataFrame = spark.sql(sql)
    val scidData: DataFrame = spark.sql(scidSQL)
    val newSongData: DataFrame = spark.sql(newSongSQL)
    val simisingerData: DataFrame = spark.sql(simiSingerSQL)
    val data: DataFrame = ratingData.join(scidData, Seq("scid"))
    println("原始数据条数：" + data.count())

    //计算每个用户分值最高的N个歌手
    val topSingerData: DataFrame = getTopSinger(data, 15)

    //用户每个歌手关联得到K个(最大)相似歌手
    val topWithsimiSingerData: DataFrame = topSingerData.join(simisingerData, "singerid")
    val joinWithSimiSingerData = calcWithSimiSinger(topWithsimiSingerData, 20, 0.85)
    println("与相似歌手关联后数据条数：" + joinWithSimiSingerData.count())

    //过滤新歌，生成歌手-歌曲map
    val singerScidMap: Map[String, List[(String, Long)]] = scidData.join(newSongData, "scid").rdd.map{row =>
      val scid: String = row.getString(0)
      val singerid: String = row.getString(1)
      val index: Long = row.getLong(2)
      (singerid, List((scid, index)))
    }.reduceByKey(_ ::: _).collect().toMap
    val singerScidMapBC: Broadcast[Map[String, List[(String, Long)]]] = spark.sparkContext.broadcast(singerScidMap)

    //评分最高的歌手取N首歌，按排名递减歌曲数
    val recommend: DataFrame = recommendSimiScids(spark, joinWithSimiSingerData, singerScidMapBC, 6)

    recommend.createOrReplaceTempView("tmpTable")
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

    import spark.implicits._
    val result: DataFrame = recommend.map{row =>
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


  def getTopSinger(data: DataFrame, num: Int): DataFrame = {
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


  def calcWithSimiSinger(data: DataFrame, maxNum: Int = 10, threshold: Double = 0.9): RDD[(String, String, List[String], Int, String, Double)] = {
    data.rdd.flatMap{row =>
      val singerid: String = row.getString(0)
      val userid: String = row.getString(1)
      val index: Int = row.getInt(2)
      val scidList: List[String] = row.getAs[mutable.WrappedArray[String]](3).toList
      val simisingers: mutable.WrappedArray[String] = row.getAs[mutable.WrappedArray[String]](4)

      // 按排名取对应相似歌手数，最大取maxNum个
      val simiNum: Int = if (maxNum - (index/2) > 2) maxNum - (index/2) else 2

      // 相似歌手取top simiNum
      val singerList: List[(String, List[(String, List[String], Int, String, Double)])] = simisingers.take(simiNum).toList.flatMap{item =>
        val tokens: Array[String] = item.split("_")

        if(tokens.length > 1){
          val simisinger: String = tokens(0)
          val rating: Double = tokens(1).toDouble

          if(rating>=threshold) {Some((userid, List((singerid, scidList, index, simisinger, rating))))} else None
        } else None
      }

      singerList :+ (userid, List((singerid, scidList, index, singerid, 1.0)))
    }.reduceByKey(_ ::: _, 1000).flatMap{record =>
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
    }
  }


  def recommendSimiScids(
                          spark: SparkSession,
                          data: RDD[(String, String, List[String], Int, String, Double)],
                          singerScidMapBC: Broadcast[Map[String, List[(String, Long)]]],
                          maxNum: Int = 8): DataFrame = {
    val scidData: RDD[(String, String, String, String, Double)] = data.mapPartitions{iter =>
      val singerScidMap: Map[String, List[(String, Long)]] = singerScidMapBC.value
      iter.flatMap{ record =>
        val userid: String = record._1
        val origSingerid: String = record._2
        val scidSet: Set[String] = record._3.toSet
        val index: Int = record._4
        val simisinger: String = record._5
        val similarity: Double = record._6
        val singerscidList: List[(String, Int)] = if(singerScidMap.get(simisinger).nonEmpty) singerScidMap(simisinger).sortBy(_._2).take(15).map(_._1).zipWithIndex else Nil
        //歌手歌曲总数
        val size: Int = singerscidList.size

        if (size > 0) {
          //根据simiNum确定歌手取的歌曲数
          var count: Int = if (maxNum - index / 2 > 4) maxNum - index / 2 else 4

          var tmpSet: Set[String] = Set()
          var tmpList: List[(String, Double)] = Nil
          if (size > count) {
            while (count > 0) {
              val tmpIndex: Int = Random.nextInt(size)
              val (tmpScid, scidIndex) = singerscidList(tmpIndex)
              val score: Double = 1 - scidIndex.toDouble / 20
              if (origSingerid == simisinger && !scidSet.contains(tmpScid) && !tmpSet.contains(tmpScid)) {
                tmpSet += tmpScid
                tmpList :+= (tmpScid, similarity * score)
                count -= 1
              } else if (!tmpSet.contains(tmpScid)) {
                tmpSet += tmpScid
                tmpList :+= (tmpScid, similarity * score)
                count -= 1
              }
            }
          } else {
            singerscidList.foreach { item =>
              val score: Double = 1 - item._2.toDouble / 20
              tmpSet += item._1
              tmpList :+= (item._1, similarity * score)
            }
          }

          tmpList.map(item => (userid, origSingerid, simisinger, item._1, item._2))
        } else None
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
