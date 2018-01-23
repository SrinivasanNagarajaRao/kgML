package net.kugou.applications.nianzhong

import net.kugou.pipeline.Segmenter
import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  *
  * Created by yhao on 2018/01/08 16:24.
  */
object GenTmpLyric extends Serializable {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val lyricSQL: String = "select trim(scid) as scid, trim(lyric) as lyric from common.canal_LyricDB_k_song_file_lyric_part where dt='2018-01-02' and lyric is not null"
    val lyricData: DataFrame = spark.sql(lyricSQL)

    import spark.implicits._
    val tmpData = lyricData.rdd.map{row =>
      val scid: String = row.getString(0)
      val lyric: String = row.getString(1)
      val lyricList: List[String] = lyric.split("@BI_FLIELD_R@@BI_COLUMN_SPLIT@").toList.map(_.replaceAll("@BI_FIELD_R@", "").replaceAll("\t", " "))

      (scid, List(lyricList.mkString(" ")))
    }.reduceByKey(_ ::: _).map{pair =>
      val scid: String = pair._1
      val lyric: String = pair._2.filter(_.nonEmpty).head
      (scid, lyric)
    }.toDF("scid", "lyric")

    val segmenter: Segmenter = new Segmenter()
      .setInputCol("lyric")
      .setOutputCol("seg_lyric")
      .setSegType("StandardSegment")
    val segedData: DataFrame = segmenter.transform(tmpData)

    val posPath: String = "hdfs://kgdc/user/haoyan/conf/positive.txt"
    val negPath: String = "hdfs://kgdc/user/haoyan/conf/negative.txt"
    val posVocab: Array[String] = spark.sparkContext.textFile(posPath).collect()
    val negVocab: Array[String] = spark.sparkContext.textFile(negPath).collect()
    val posVocabBC: Broadcast[Array[String]] = spark.sparkContext.broadcast(posVocab)   //广播变量
    val negVocabBC: Broadcast[Array[String]] = spark.sparkContext.broadcast(negVocab)   //广播变量

    import spark.implicits._
    val result: DataFrame = segedData.select("scid", "seg_lyric").map{row =>
      val scid: String = row.getString(0)
      val tokens: Seq[String] = row.getAs[Seq[String]](1)

      val posMap: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()
      val negMap: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()

      tokens.foreach{token =>
        if (posVocabBC.value.contains(token)) {
          if(posMap.get(token).nonEmpty) {
            val newPv: Long = posMap(token) + 1L
            posMap.put(token, newPv)
          } else posMap.put(token, 1L)

        } else if (negVocabBC.value.contains(token)) {
          if(negMap.get(token).nonEmpty) {
            val newPv: Long = negMap(token) + 1L
            negMap.put(token, newPv)
          } else negMap.put(token, 1L)
        }
      }

      val posList: List[(String, Long)] = posMap.toList
      val negList: List[(String, Long)] = negMap.toList
      val posSum: Long = posList.map(_._2).sum
      val negSum: Long = negList.map(_._2).sum

      val posStr: String = posList.map(pair => pair._1 + "," + pair._2).mkString(";")
      val negStr: String = negList.map(pair => pair._1 + "," + pair._2).mkString(";")

      (scid, posSum, negSum, posStr, negStr)
    }.toDF("scid", "sum_pos", "sum_neg", "word_pos", "word_neg")

    result.createOrReplaceTempView("tmpTable")

    val createSQL: String = "create table if not exists mllab.lyric_for_emotion_filter( " +
      "scid string, " +
      "sum_pos bigint, " +
      "sum_neg bigint, " +
      "word_pos string, " +
      "word_neg string" +
      ") ROW FORMAT DELIMITED  " +
      "FIELDS TERMINATED BY '|'  " +
      "STORED AS TEXTFILE "

    val insertSQL: String = "insert into mllab.lyric_for_emotion_filter select * from tmpTable"

    spark.sql(createSQL)
    spark.sql(insertSQL)

    spark.stop()
  }
}
