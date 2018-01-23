package net.kugou.applications.nianzhong

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * Created by yhao on 2018/01/11 19:19.
  */
object EmotionSummarizerNext extends Serializable {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val parts: String = args(0)
    val partsStr: String = parts.split(",").map(part => "\'" + part + "\'").mkString(",")

    val sql: String = "select userid, word_pos, word_neg, lyric_pos, lyric_neg, partstr " +
      s"from mllab.summarize_user_emotion_tmp where partstr in ($partsStr)"

    import spark.implicits._
    val result: DataFrame = spark.sql(sql).rdd.map{row =>
      val userid: String = row.getString(0)
      val partStr: String = row.getString(5)

      val posWordList: Array[(String, Long)] = row.getString(1).split(";").flatMap{item =>
        val tokens: Array[String] = item.split(",")
        if (tokens.length > 1) Some((tokens(0), tokens(1).toLong)) else None
      }

      val negWordList: Array[(String, Long)] = row.getString(2).split(";").flatMap{item =>
        val tokens: Array[String] = item.split(",")
        if (tokens.length > 1) Some((tokens(0), tokens(1).toLong)) else None
      }

      val posLyricList: Array[(String, String)] = row.getString(3).split("@LYRIC@").flatMap{item =>
        val tokens: Array[String] = item.split("@INNER@")
        if (tokens.length > 1) Some((tokens(0), tokens(1))) else None
      }

      val negLyricList: Array[(String, String)] = row.getString(4).split("@LYRIC@").flatMap{item =>
        val tokens: Array[String] = item.split("@INNER@")
        if (tokens.length > 1) Some((tokens(0), tokens(1))) else None
      }

      var posList: List[(String, Long, String, String)] = List()
      if (posWordList.nonEmpty && posWordList.length == posLyricList.length) {
        posList = posWordList.zip(posLyricList).map{record =>
          val word: String = record._1._1
          val count: Long = record._1._2
          val scid: String = record._2._1
          val lyric: String = record._2._2
          (word, count, scid, lyric)
        }.toList
      }

      var negList: List[(String, Long, String, String)] = List()
      if (negWordList.nonEmpty && negWordList.length == negLyricList.length) {
        negList = negWordList.zip(negLyricList).map{record =>
          val word: String = record._1._1
          val count: Long = record._1._2
          val scid: String = record._2._1
          val lyric: String = record._2._2
          (word, count, scid, lyric)
        }.toList
      }

      val resultList: List[(String, Long, String, String)] = posList.:::(negList).sortBy(-_._2).take(10)

      val wordStr: String = resultList.map(item => item._1 + "," + item._2).mkString(";")
      val lyricStr: String = resultList.map(item => item._3 + "##" + item._4).mkString(";;")

      (userid, wordStr, lyricStr, partStr)
    }.toDF("userid", "words", "lyrics", "partstr")

    result.createOrReplaceTempView("tmpTable")

    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.max.dynamic.partitions=100000")
    spark.sql("set hive.exec.max.dynamic.partitions.pernode=100000")

    val createSQL: String = "create table if not exists mllab.summarize_user_keyword( " +
      "userid          string COMMENT '用户ID', " +
      "words           string COMMENT '情感词', " +
      "lyrics          string COMMENT '情感词对应歌词' " +
      ") " +
      "PARTITIONED BY (partstr string)" +
      "ROW FORMAT DELIMITED  " +
      "FIELDS TERMINATED BY '|'  " +
      "STORED AS TEXTFILE "

    val insertSQL: String = "insert overwrite table mllab.summarize_user_keyword partition(partstr) " +
      "select userid, words, lyrics, partstr from tmpTable"

    spark.sql(createSQL)
    spark.sql(insertSQL)

    spark.stop()
  }
}
