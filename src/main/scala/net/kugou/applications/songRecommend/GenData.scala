package net.kugou.applications.songRecommend

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

/**
  * Author: yhao
  * Time: 2018/07/12 11:06
  * Desc: 
  *
  */
object GenData extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()
/*

    val tableName: String = "mllab.user_scidlist_score"
    val userData: RDD[(String, List[(String, Double)], Int)] = parseDataNew(spark, tableName, 20)

    //--- 保存临时中间结果表
    import spark.implicits._
    val tmpData: DataFrame = userData.map{record =>
      val userid: String = record._1
      val count: Int = record._3
      val songStr: String = record._2.map(item => item._1 + "_" + item._2).mkString(",")

      (userid, count, songStr)
    }.toDF("userid", "count", "scidlist")
    tmpData.createOrReplaceTempView("tmpTable")

    spark.sql("use temp")
    spark.sql("create table temp.user_scidlist_score_filter as select userid, count, scidlist from tmpTable")
*/


    val tableName: String = "temp.user_scidlist_score_filter"
    val sql: String = "select userid, max(scidlist) scidlist from (" +
      s"    (select userid, scidlist from $tableName where (count>30 and count<800) order by rand(1234) limit 5000000)" +
      "    union all " +
      s"    (select userid, scidlist from $tableName where userid in ('115014057', '1152685307', '1139559310', '1017404770', '1272958046', '574553698', '985354155'))" +
      ") t1 group by t1.userid"

    var userItemRatingData: DataFrame = genRatingDataNew(spark, sql, lowThreshold = 10, highThreshold = 1000)

//    val sampleSQL: String = s"select userid, scidlist from $tableName where userid in ('115014057', '1152685307', '1139559310', '1017404770', '1272958046', '574553698', '985354155')"
//    val sampleItemRatingData: DataFrame = genRatingDataNew(spark, sampleSQL)
//    userItemRatingData = userItemRatingData.union(sampleItemRatingData)

    userItemRatingData.createOrReplaceTempView("tmpTable")
    spark.sql("use temp")
    spark.sql("create table temp.tb_user_item_rating_500w_new as select userid, scid, rating from tmpTable")

    spark.stop()
  }


  /**
    * 解析用户资产表数据，生成按条件过滤后的用户资产数据
    *
    * @param spark  SparkSession
    * @param tableName  输入表
    * @param threshold  过滤条件
    * @return
    */
  def parseData(spark: SparkSession, tableName: String, threshold: Int) = {
    val data: DataFrame = spark.read.table(tableName)

    val userData: RDD[(String, List[(String, String, Int, Int, Int)], Int)] = data.rdd.flatMap { row =>
      val userid: String = if (Option(row.get(0)).nonEmpty) row.getString(0) else ""
      val songStr: String = if (Option(row.get(1)).nonEmpty) row.getString(1) else ""

      if (songStr.nonEmpty && userid.nonEmpty && !userid.contains(".") && userid!='0') {
        val songList: List[String] = songStr.split(",").toList

        var newSongList: List[(String, String, Int, Int, Int)] = Nil
        songList.foreach{song =>
          val tokens: Array[String] = song.split("_")
          if(tokens.length>4) {
            val scid: String = tokens(0)
            val date: String = tokens(1)
            val playCnt: Int = tokens(2).toInt
            val play30Cnt: Int = tokens(3).toInt
            val play90Cnt: Int = tokens(4).toInt

            if (playCnt!=0 || play30Cnt!=0 || play90Cnt!=0) {
              newSongList :+= (scid, date, playCnt, play30Cnt, play90Cnt)
            }
          }
        }

        // 过滤有效资产数小于threshold的用户
        if (newSongList.size>=threshold) {
          Some((userid, newSongList, newSongList.size))
        } else None
      } else None
    }

    userData
  }


  def parseDataNew(spark: SparkSession, tableName: String, threshold: Int) = {
    val data: DataFrame = spark.sql(s"select userid, scidlist from $tableName where dt='2018-07-16'")

    val userData: RDD[(String, List[(String, Double)], Int)] = data.rdd.flatMap { row =>
      val userid: String = if (Option(row.get(0)).nonEmpty) row.getString(0) else ""
      val scidStr = if (Option(row.get(1)).nonEmpty) row.getString(1) else ""
      val scidArray: Array[String] = scidStr.split(",")

      if (scidArray.nonEmpty && userid.nonEmpty && !userid.contains(".") && userid!='0') {
        var newSongList: List[(String, Double)] = Nil
        scidArray.foreach{song =>
          val tokens: Array[String] = song.split("_")
          if(tokens.length>1) {
            val scid: String = tokens(0)
            val rating: Double = tokens(1).toDouble
            newSongList :+= (scid, rating)
          }
        }

        // 过滤有效资产数小于threshold的用户
        if (newSongList.size>=threshold) {
          Some((userid, newSongList, newSongList.size))
        } else None
      } else None
    }

    userData
  }


  def genRatingData(spark: SparkSession, sql: String) = {
    val data: DataFrame = spark.sql(sql)

    val songListData: RDD[(String, List[(String, String, Int, Int, Int)], Int)] = data.rdd.map{row =>
      val userid: String = row.getString(0)
      val count: Int = row.getInt(1)

      val songList: List[(String, String, Int, Int, Int)] = row.getString(2).split(",").flatMap{item =>
        val tokens: Array[String] = item.split("_")

        if (tokens.length>4) {
          val scid: String = tokens(0)
          val date: String = tokens(1)
          val playCnt: Int = tokens(2).toInt
          val play30Cnt: Int = tokens(3).toInt
          val play90Cnt: Int = tokens(4).toInt
          Some((scid, date, playCnt, play30Cnt, play90Cnt))
        } else None
      }.toList

      (userid, songList, count)
    }

    import spark.implicits._
    val result: DataFrame = songListData.flatMap{record =>
      val userid: Int = record._1.toInt
      record._2.map(item => (userid, item._1.toInt, item._3, item._4, item._5))
    }.map{ record =>
      val (userid, scid, playCnt, play30Cnt, play90Cnt) = record

      // 评分，未播放歌曲:0; 播放小于30%:-2; 播放小于90%:2; 播放大于90%:5
      // val rating: Double = if (playCnt==0 && play30Cnt==0 && play90Cnt==0) 0.0 else -2.0*playCnt + 2.0*play30Cnt + 5.0*play90Cnt
      val rating: Double = -2.0*playCnt + 1.0*play30Cnt + 3.0*play90Cnt
      (userid, scid, rating)
    }.toDF("userid", "scid", "rating")

    result
  }


  def genRatingDataNew(spark: SparkSession, sql: String, lowThreshold: Int = 1, highThreshold: Int = Int.MaxValue) = {
    val data: DataFrame = spark.sql(sql)
    println("加载数据条数：" + data.count())

    val songListData: RDD[(Int, Int, Double)] = data.rdd.flatMap{row =>
      val userid: String = row.getString(0)
      val songArray: Array[String] = row.getString(1).split(",")

      if (songArray.length>=lowThreshold && songArray.length<highThreshold) {
        songArray.flatMap{item =>
          val tokens: Array[String] = item.split("_")

          if (tokens.length>1) {
            val scid: String = tokens(0)
            val rating: Double = tokens(1).toDouble

            Some((userid.toInt, scid.toInt, rating))
          } else None
        }.toList
      } else None
    }

    import spark.implicits._
    songListData.map{record =>
      (record._2, List((record._1, record._3)))
    }.reduceByKey(_ ::: _).filter(_._2.size > 100).flatMap{record =>
      record._2.map(item => (item._1, record._1, item._2))
    }.filter(_._3 > 7).toDF("userid", "scid", "rating")
  }
}
