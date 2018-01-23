import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  *
  * Created by yhao on 2018/01/11 15:17.
  */
object EmotionResultFilter extends Serializable {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val parts: String = args(0)
    val partsStr: String = parts.split(",").map(part => "\'" + part + "\'").mkString(",")
    val sql: String = s"select userid, nature, words, partstr from mllab.summarize_user_emotion_words_new where partstr in ($partsStr)"

    import spark.implicits._
    val data: DataFrame = spark.sql(sql).rdd.map{row =>
      val userid: String = row.getString(0)
      val nature: String = row.getString(1)
      val wordList: List[String] = row.getString(2).split(";;").toList
      val partStr: String = row.getString(3)

      val lyricSet: mutable.HashSet[String] = new mutable.HashSet[String]()
      val newWordList: List[String] = wordList.filter{item =>
        val tokens: Array[String] = item.split("##")
        if (!lyricSet.contains(tokens(3))) {
          lyricSet.add(tokens(3))
          true
        } else false
      }

      //是否经过过滤
      val flag: String = if (wordList.size == newWordList.size) "0" else "1"

      (userid, nature, newWordList.mkString(";;"), flag, partStr)
    }.toDF("userid", "nature", "words", "flag", "partstr")

    data.createOrReplaceTempView("tmpTable")

    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.max.dynamic.partitions=100000")
    spark.sql("set hive.exec.max.dynamic.partitions.pernode=100000")

    val createSQL: String = "create table if not exists mllab.summarize_user_emotion_words_f_new( " +
      "userid          string COMMENT '用户ID', " +
      "nature          string COMMENT '情感倾向', " +
      "words           string COMMENT '情感词', " +
      "flag            string COMMENT '1表示经过过滤' " +
      ") " +
      "PARTITIONED BY (partstr string)" +
      "ROW FORMAT DELIMITED  " +
      "FIELDS TERMINATED BY '|'  " +
      "STORED AS TEXTFILE "

    val insertSQL: String = "insert overwrite table mllab.summarize_user_emotion_words_f_new partition(partstr) " +
      "select userid, nature, words, flag, partstr from tmpTable"

    spark.sql(createSQL)
    spark.sql(insertSQL)

    spark.stop()
  }
}
