package net.kugou.applications.nianzhong

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * 年终盘点——用户听歌最多的歌词
  * 要求：计算用户正负向top5情感词以及pv，每个情感倾向pv总和最多的倾向作为最后用户情感倾向
  *
  * Created by yhao on 2018/1/3/003 18:05.
  */
object EmotionSummarizer extends Serializable {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val parts: String = args(0)
    val partsStr: String = parts.split(",").map(part => "\'" + part + "\'").mkString(",")

//    val sql: String = s"select user_id as userid, scid, audio_full_play as play_pv, sum_pos, sum_neg, word_pos, word_neg, partstr " +
//      s"from mllab.tab_user_play_lyric_stat where partstr in ($partsStr)"

    val sql: String = s"select user_id as userid, scid, audio_full_play as play_pv, sum_pos, sum_neg, word_pos, word_neg, partstr " +
      s"from mllab.tab_user_play_lyric_stat_filter where partstr in ($partsStr)"
    println("Hive查询SQL:")
    println(sql)

    val data = spark.sql(sql).rdd.map{row =>
      val userid: String = if (Option(row.get(0)).nonEmpty) row.getString(0) else ""
      val scid: String = if (Option(row.get(1)).nonEmpty) row.getString(1) else ""
      val pv: Long = if (Option(row.get(2)).nonEmpty) row.getLong(2) else 0L
      val posSum: Long = if (Option(row.get(3)).nonEmpty) row.getLong(3) else 0L
      val negSum: Long = if (Option(row.get(4)).nonEmpty) row.getLong(4) else 0L
      val posWord: String = if (Option(row.get(5)).nonEmpty) row.getString(5) else ""
      val negWord: String = if (Option(row.get(6)).nonEmpty) row.getString(6) else ""
      val partstr: String = if (Option(row.get(7)).nonEmpty) row.getString(7) else ""
      val trend: Long = (posSum - negSum) * pv

      val posWordList: List[(String, Long)] = posWord.split(";").flatMap{item =>
        val tokens: Array[String] = item.split(",")
        if (tokens.length > 1) Some((tokens(0), tokens(1).toLong)) else None
      }.map(pair => (pair._1, pair._2 * pv)).toList

      val negWordList: List[(String, Long)] = negWord.split(";").flatMap{item =>
        val tokens: Array[String] = item.split(",")
        if (tokens.length > 1) Some((tokens(0), tokens(1).toLong)) else None
      }.map(pair => (pair._1, pair._2 * pv)).toList

      ((partstr, userid), (trend, posWordList, negWordList))
    }.reduceByKey{(a, b) =>
      val trend: Long = a._1 + b._1
      val posWordList: List[(String, Long)] = a._2 ::: b._2
      val negWordList: List[(String, Long)] = a._3 ::: b._3
      (trend, posWordList, negWordList)
    }

    import spark.implicits._
    val result = data.map{record =>
      val partstr: String = record._1._1
      val userid: String = record._1._2

      val posWordList: List[(String, Long)] = record._2._2.take(80000)     //取前8w词，避免异常播放用户的list太长
      val negWordList: List[(String, Long)] = record._2._3.take(80000)     //取前8w词，避免异常播放用户的list太长
      val posMap: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()
      val negMap: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()

      //对正向词求和
      posWordList.foreach{pair =>
        val token: String = pair._1
        val play_pv: Long = pair._2

        if(posMap.get(token).nonEmpty) {
          val newPv: Long = posMap(token) + play_pv
          posMap.put(token, newPv)
        } else posMap.put(token, play_pv)
      }

      //对负向词求和
      negWordList.foreach{pair =>
        val token: String = pair._1
        val play_pv: Long = pair._2

        if(negMap.get(token).nonEmpty) {
          val newPv: Long = negMap(token) + play_pv
          negMap.put(token, newPv)
        } else negMap.put(token, play_pv)
      }

      val posTop5: List[(String, Long)] = posMap.toList.sortBy(-_._2).take(5)
      val negTop5: List[(String, Long)] = negMap.toList.sortBy(-_._2).take(5)
      val posStr: String = posTop5.map{pair => pair._1 + "," + pair._2}.mkString(";")
      val negStr: String = negTop5.map{pair => pair._1 + "," + pair._2}.mkString(";")

      val trend: Long = posTop5.map(_._2).sum - negTop5.map(_._2).sum
      if (trend >= 0) (userid, "开心", posStr, partstr) else (userid, "伤心", negStr, partstr)
    }.toDF("userid", "nature_str", "keywords", "partstr")

    result.createOrReplaceTempView("tmpTable")

    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.max.dynamic.partitions=100000")
    spark.sql("set hive.exec.max.dynamic.partitions.pernode=100000")

    val createSQL: String = "create table if not exists mllab.summarize_user_emotion_words_filter( " +
      "userid      string COMMENT '用户ID', " +
      "nature_str  string COMMENT '情感倾向', " +
      "keywords    string COMMENT '情感词' " +
      ") " +
      "PARTITIONED BY (partstr string)" +
      "ROW FORMAT DELIMITED  " +
      "FIELDS TERMINATED BY '|'  " +
      "STORED AS TEXTFILE "

    val insertSQL: String = "insert overwrite table mllab.summarize_user_emotion_words_filter partition(partstr) select * from tmpTable"

    spark.sql(createSQL)
    spark.sql(insertSQL)

    spark.stop()
  }

}
