package net.kugou.applications.nianzhong

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * Created by yhao on 2018/1/7/007 15:02.
  */
object GenDataForEmotion {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val parts: String = args(0)
    val partsStr: String = parts.split(",").map(part => "\'" + part + "\'").mkString(",")

    val playSQL: String = s"select trim(user_id) as user_id, trim(scid) as scid, audio_full_play, partstr from temp.temp_userid_year_2017_action_partstr where partstr in ($partsStr)"
    val lyricSQL: String = "select trim(scid) as scid, trim(lyric) as lyric from common.canal_LyricDB_k_song_file_lyric_part where dt='2018-01-02'"

    val playData: DataFrame = spark.sql(playSQL)
    val lyricData: DataFrame = spark.sql(lyricSQL)

    import spark.implicits._
    val filterLyricData: DataFrame = lyricData.rdd.map{row =>
      val scid: String = row.getString(0)
      val lyric: String = row.getString(1)
      (scid, List(lyric))
    }.reduceByKey(_ ::: _).map{record =>
      val scid: String = record._1
      val lyric: String = record._2.filter(_.nonEmpty).head
      (scid, lyric)
    }.toDF("scid", "lyric")

    val data: DataFrame = playData.join(filterLyricData, Seq("scid")).na.fill(Map("user_id" -> "", "lyric" -> "")).repartition(5000)

    data.select("user_id", "scid", "audio_full_play", "lyric", "partstr").createOrReplaceTempView("tmpTable")

    val createSQL: String = "create table if not exists mllab.user_lyric_y_part ( " +
      "user_id string COMMENT '用户ID', " +
      "scid string COMMENT '歌曲scid', " +
      "audio_full_play bigint COMMENT '歌曲pv', " +
      "lyric string COMMENT '歌词' " +
      ") PARTITIONED BY (partstr string)" +
      "ROW FORMAT DELIMITED  " +
      "FIELDS TERMINATED BY '|'  " +
      "STORED AS TEXTFILE "

    val insertSQL: String = "insert into mllab.user_lyric_y_part partition(partstr) select * from tmpTable"

    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.max.dynamic.partitions=100000")
    spark.sql("set hive.exec.max.dynamic.partitions.pernode=100000")

    spark.sql(createSQL)
    spark.sql(insertSQL)

    spark.stop()
  }
}
