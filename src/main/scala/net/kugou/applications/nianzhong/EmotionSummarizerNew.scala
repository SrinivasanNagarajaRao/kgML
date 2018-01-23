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
object EmotionSummarizerNew extends Serializable {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val parts: String = args(0)
    val partsStr: String = parts.split(",").map(part => "\'" + part + "\'").mkString(",")

    val sql: String = s"select user_id as userid, scid, audio_full_play as play_pv, word_pos_top5, word_neg_top5, lyric_pos_top5, lyric_neg_top5, partstr " +
      s"from mllab.tab_user_play_lyric_stat_new where partstr in ($partsStr)"
    println("Hive查询SQL:")
    println(sql)

    val data = spark.sql(sql).rdd.map{row =>
      val userid: String = if (Option(row.get(0)).nonEmpty) row.getString(0) else ""
      val scid: String = if (Option(row.get(1)).nonEmpty) row.getString(1) else ""
      val pv: Long = if (Option(row.get(2)).nonEmpty) row.getLong(2) else 0L
      val posWord: String = if (Option(row.get(3)).nonEmpty) row.getString(3) else ""
      val negWord: String = if (Option(row.get(4)).nonEmpty) row.getString(4) else ""
      val posLyric: String = if (Option(row.get(5)).nonEmpty) row.getString(5) else ""
      val negLyric: String = if (Option(row.get(6)).nonEmpty) row.getString(6) else ""
      val partstr: String = if (Option(row.get(7)).nonEmpty) row.getString(7) else ""

      val posWordList: List[(String, Long)] = posWord.split(";").flatMap{item =>
        val tokens: Array[String] = item.split(",")
        if (tokens.length > 1) Some((tokens(0), tokens(1).toLong)) else None
      }.map(pair => (pair._1, pair._2 * pv)).toList

      val negWordList: List[(String, Long)] = negWord.split(";").flatMap{item =>
        val tokens: Array[String] = item.split(",")
        if (tokens.length > 1) Some((tokens(0), tokens(1).toLong)) else None
      }.map(pair => (pair._1, pair._2 * pv)).toList

      val trend: Long = posWordList.map(_._2).sum - negWordList.map(_._2).sum     //歌曲倾向，>=0 表示正向，<0 表示负向

      val posLyricList: List[(String, Long, String)] = posLyric.split("@LYRIC@").flatMap{item =>
        val tokens: Array[String] = item.split("@COUNT@")
        if (tokens.length > 1) Some((tokens(0), tokens(1).toLong, scid)) else None
      }.map(pair => (pair._1, pair._2 * pv, pair._3)).toList

      val negLyricList: List[(String, Long, String)] = negLyric.split("@LYRIC@").flatMap{item =>
        val tokens: Array[String] = item.split("@COUNT@")
        if (tokens.length > 1) Some((tokens(0), tokens(1).toLong, scid)) else None
      }.map(pair => (pair._1, pair._2 * pv, pair._3)).toList

      //正负向情感词和歌词进行合并
      var posResultList: List[(String, Long, String, Long, String)] = List()    //正向词list
      var negResultList: List[(String, Long, String, Long, String)] = List()    //负向词list

      if (posWordList.size == posLyricList.size) {
        posWordList.zip(posLyricList).foreach{ record =>
          posResultList :+= (record._1._1, record._1._2, record._2._1, record._2._2, record._2._3)
        }
      }

      if (negWordList.size == negLyricList.size) {
        negWordList.zip(negLyricList).foreach({record =>
          negResultList :+= (record._1._1, record._1._2, record._2._1, record._2._2, record._2._3)
        })
      }

      ((partstr, userid), (trend, posResultList, negResultList))
    }.reduceByKey{(a, b) =>
      val trend: Long = a._1 + b._1
      val posWordList: List[(String, Long, String, Long, String)] = a._2 ::: b._2
      val negWordList: List[(String, Long, String, Long, String)] = a._3 ::: b._3
      (trend, posWordList, negWordList)
    }


    val filterData = data.map { record =>
      val partstr: String = record._1._1
      val userid: String = record._1._2
      val trend: Long = record._2._1

      val posWordList: List[(String, Long, String, Long, String)] = record._2._2.take(20000) //取前8w词，避免异常播放用户的list太长
      val negWordList: List[(String, Long, String, Long, String)] = record._2._3.take(20000) //取前8w词，避免异常播放用户的list太长

      val posMap = new mutable.HashMap[String, (Long, List[(String, Long, String)])]()
      val negMap = new mutable.HashMap[String, (Long, List[(String, Long, String)])]()

      //对正向词求和, 取count最大的歌词作为当前词的歌词
      posWordList.foreach { item =>
        val token: String = item._1
        val tmpPv: Long = item._2
        val tmpLyric: String = item._3
        val tmpCount: Long = item._4
        val tmpScid: String = item._5

        if (posMap.get(token).nonEmpty) {
          val (pv, oldLyricList) = posMap(token)
          val oldLyricSet: Set[String] = oldLyricList.map(_._1).toSet

          val newPv: Long = pv + tmpPv
          if (!oldLyricSet.contains(tmpLyric) && oldLyricSet.size <= 10) posMap.put(token, (newPv, oldLyricList :+ (tmpLyric, tmpCount, tmpScid))) //取count最大的歌词作为当前词的歌词
          else posMap.put(token, (newPv, oldLyricList))
        } else {
          posMap.put(token, (tmpPv, List((tmpLyric, tmpCount, tmpScid))))
        }
      }

      //对负向词求和, 取count最大的歌词作为当前词的歌词
      negWordList.foreach { item =>
        val token: String = item._1
        val tmpPv: Long = item._2
        val tmpLyric: String = item._3
        val tmpCount: Long = item._4
        val tmpScid: String = item._5

        if (negMap.get(token).nonEmpty) {
          val (pv, oldLyricList) = negMap(token)
          val oldLyricSet: Set[String] = oldLyricList.map(_._1).toSet

          val newPv: Long = pv + tmpPv
          if (!oldLyricSet.contains(tmpLyric) && oldLyricSet.size < 10) negMap.put(token, (newPv, oldLyricList :+ (tmpLyric, tmpCount, tmpScid))) //取count最大的歌词作为当前词的歌词
          else negMap.put(token, (newPv, oldLyricList))
        } else {
          negMap.put(token, (tmpPv, List((tmpLyric, tmpCount, tmpScid))))
        }
      }

      //正负情感词各取前5个
      val posTop10: List[(String, (Long, List[(String, Long, String)]))] = posMap.toList.sortBy(-_._2._1).take(10)
      val negTop10: List[(String, (Long, List[(String, Long, String)]))] = negMap.toList.sortBy(-_._2._1).take(10)

      (userid, trend, posTop10, negTop10, partstr)
    }

    import spark.implicits._
    val result = filterData.map {record =>
      val userid: String = record._1
      val trend: Long = record._2
      val partStr: String = record._5

      val posTop10: List[(String, (Long, List[(String, Long, String)]))] = record._3
      val negTop10: List[(String, (Long, List[(String, Long, String)]))] = record._4

      //过滤，不同心情词中不能出现相同的歌词
      val posLyricSet: mutable.HashSet[String] = new mutable.HashSet[String]()
      val posResult: List[(String, (Long, String, Long, String))] = posTop10.flatMap{item =>
        val tmpWord: String = item._1
        val tmpCount: Long = item._2._1
        val tmpFilterList = item._2._2.filter{a => !posLyricSet.contains(a._1)}

        if (tmpFilterList.nonEmpty) {
          val (tmpLyric, tmpLyricCount, tmpScid) = tmpFilterList.maxBy(_._2)
          posLyricSet.add(tmpLyric)
          Some((tmpWord, (tmpCount, tmpLyric, tmpLyricCount, tmpScid)))
        } else None
      }

      //过滤，不同心情词中不能出现相同的歌词
      val negLyricSet: mutable.HashSet[String] = new mutable.HashSet[String]()
      val negResult: List[(String, (Long, String, Long, String))] = negTop10.flatMap{item =>
        val tmpWord: String = item._1
        val tmpCount: Long = item._2._1
        val tmpFilterList = item._2._2.filter{a => !negLyricSet.contains(a._1)}

        if (tmpFilterList.nonEmpty) {
          val (tmpLyric, tmpLyricCount, tmpScid) = tmpFilterList.maxBy(_._2)
          negLyricSet.add(tmpLyric)
          Some((tmpWord, (tmpCount, tmpLyric, tmpLyricCount, tmpScid)))
        } else None
      }

      val nature: String = if (trend >= 0) "开心" else "伤心"
      val posWordStr: String = posResult.map{pair => pair._1 + "," + pair._2._1}.mkString(";")
      val negWordStr: String = negResult.map{pair => pair._1 + "," + pair._2._1}.mkString(";")
      val posLyricStr: String = posResult.map{pair => pair._2._4 + "@INNER@" + pair._2._2 + "@INNER@" + pair._2._3}.mkString("@LYRIC@")
      val negLyricStr: String = negResult.map{pair => pair._2._4 + "@INNER@" + pair._2._2 + "@INNER@" + pair._2._3}.mkString("@LYRIC@")

      (userid, nature, posWordStr, negWordStr, posLyricStr, negLyricStr, partStr)

      /*val nature: String = if (trend >= 0) "1" else "2"

      val wordStr: String = if (trend > 0) {
        posTop5.sortBy(-_._2._1).take(5).map{item =>
          item._2._4 + "##" + item._2._1 + "##" + item._1 + "##" + item._2._2.replaceAll("，", " ").replaceAll(",", " ")
        }.mkString(";;")
      } else {
        negTop5.sortBy(-_._2._1).take(5).map{item =>
          item._2._4 + "##" + item._2._1 + "##" + item._1 + "##" + item._2._2.replaceAll("，", " ").replaceAll(",", " ")
        }.mkString(";;")
      }

      (userid, nature, wordStr, partstr)*/
    }.toDF("userid", "nature", "word_pos", "word_neg", "lyric_pos", "lyric_neg", "partstr")

    result.createOrReplaceTempView("tmpTable")

    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.max.dynamic.partitions=100000")
    spark.sql("set hive.exec.max.dynamic.partitions.pernode=100000")

    val createSQL: String = "create table if not exists mllab.summarize_user_emotion_tmp( " +
      "userid          string COMMENT '用户ID', " +
      "nature          string COMMENT '情感倾向', " +
      "word_pos        string COMMENT '正向情感词', " +
      "word_neg        string COMMENT '负向情感词', " +
      "lyric_pos       string COMMENT '正向词对应歌词', " +
      "lyric_neg       string COMMENT '负向词对应歌词' " +
      ") " +
      "PARTITIONED BY (partstr string)" +
      "ROW FORMAT DELIMITED  " +
      "FIELDS TERMINATED BY '|'  " +
      "STORED AS TEXTFILE "

    val insertSQL: String = "insert overwrite table mllab.summarize_user_emotion_tmp partition(partstr) " +
      "select userid, nature, word_pos, word_neg, lyric_pos, lyric_neg, partstr from tmpTable"

    spark.sql(createSQL)
    spark.sql(insertSQL)

    spark.stop()
  }

}
