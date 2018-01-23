import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * Created by yhao on 2017/10/25 10:48.
  */
object Test {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val parts: String = args(0)
    val partsStr: String = parts.split(",").map(part => "\'" + part + "\'").mkString(",")

    val sql: String = s"select userid, nature_str, keywords, partstr from mllab.summarize_user_emotion_words_filter where partstr in ($partsStr)"
    val data: DataFrame = spark.sql(sql)

    val negDropDic: Array[String] = spark.sparkContext.textFile("hdfs://kgdc/user/haoyan/conf/neg_word_for_drop.txt").collect()
    val negDropDicBC: Broadcast[Array[String]] = spark.sparkContext.broadcast(negDropDic)

    val result = data.rdd.flatMap{row =>
      val userid: String = row.getString(0)
      val nature: String = row.getString(1)
      val wordList: List[(String, Long)] = row.getString(2).split(";").flatMap{token =>
        val items: Array[String] = token.split(",")
        if (items.length > 1) Some((items(0), items(1).toLong)) else None
      }.toList
      val partstr: String = row.getString(3)

      if (nature.equals("伤心")) {
        val rowNum: Long = 1L
        var conRowNum: Long = 0L
        val wordNum: Long = wordList.size
        var conWordNum: Long = 0L

        wordList.map(_._1).foreach{word =>
          if (negDropDicBC.value.contains(word)) {
            conRowNum = 1L
            conWordNum += 1L
          }
        }

        Some((partstr, (conRowNum, rowNum, conWordNum, wordNum)))
      } else None
    }.reduceByKey{(a, b) =>
      val conRowNum: Long = a._1 + b._1
      val rowNum: Long = a._2 + b._2
      val conWordNum: Long = a._3 + b._3
      val wordNum: Long = a._4 + b._4
      (conRowNum, rowNum, conWordNum, wordNum)
    }

    result.collect().foreach(println)


    spark.stop()
  }
}
