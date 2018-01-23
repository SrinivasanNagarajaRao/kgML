package net.kugou.applications.nianzhong

import net.kugou.pipeline.Segmenter
import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  *
  * Created by yhao on 2018/01/10 11:52.
  */
object GenTmpLyricNew extends Serializable {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val lyricSQL: String = "select scid, lyric, rank from ( " +
      "select t1.scid, t1.lyric, row_number() over(distribute by t1.scid sort by t1.lyric desc) as rank  " +
      "from (select trim(scid) as scid, trim(lyric) as lyric from common.canal_LyricDB_k_song_file_lyric_part " +
      "where dt='2018-01-02' and lyric is not null) t1 ) t2 where rank=1 "
    val lyricData: DataFrame = spark.sql(lyricSQL)

    import spark.implicits._
    val data = lyricData.rdd.flatMap{row =>
      val scid: String = row.getString(0)
      val lyric: String = row.getString(1)
      val lyricList: List[String] = lyric.split("@BI_FIELD_R@@BI_COLUMN_SPLIT@").toList.map(_.replaceAll("@BI_FIELD_R@", "").replaceAll("\t", " "))

      val lyricMap: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()
      lyricList.foreach{sentence =>
        if (lyricMap.get(sentence).nonEmpty) lyricMap.put(sentence, lyricMap(sentence) + 1L)
        else lyricMap.put(sentence, 1L)
      }

      lyricMap.toList.map(pair => (scid, pair._1, pair._2))
    }.toDF("scid", "sentence", "count")

    val segmenter: Segmenter = new Segmenter()
      .setInputCol("sentence")
      .setOutputCol("seg_sentence")
      .setSegType("StandardSegment")
    val segedData: DataFrame = segmenter.transform(data)

    val stopwords: Array[String] = spark.sparkContext.textFile("hdfs://kgdc/user/haoyan/conf/stopword_zh.dic").collect()
    val remover: StopWordsRemover = new StopWordsRemover()
      .setInputCol(segmenter.getOutputCol)
      .setOutputCol("remove_sentence")
      .setStopWords(stopwords)
    val removedData: DataFrame = remover.transform(segedData)

    val posPath: String = "hdfs://kgdc/user/haoyan/conf/positive.txt"
    val negPath: String = "hdfs://kgdc/user/haoyan/conf/negative.txt"
    val posVocab: Array[String] = spark.sparkContext.textFile(posPath).collect()
    val negVocab: Array[String] = spark.sparkContext.textFile(negPath).collect()
    val posVocabBC: Broadcast[Array[String]] = spark.sparkContext.broadcast(posVocab)   //广播变量
    val negVocabBC: Broadcast[Array[String]] = spark.sparkContext.broadcast(negVocab)   //广播变量

    val transformedData = removedData.select("scid", "sentence", "remove_sentence", "count").rdd.flatMap{row =>
      val scid: String = row.getString(0)
      val sentence: String = row.getString(1)
      val wordList: Seq[String] = row.getAs[Seq[String]](2)
      val count: Long = row.getLong(3)

      val posWordMap = new mutable.HashMap[String, (String, Long, Long)]()    //(情感词, (句子, 句子次数, 词次数))
      val negWordMap = new mutable.HashMap[String, (String, Long, Long)]()    //(情感词, (句子, 句子次数, 词次数))
      wordList.foreach{word =>
        if (posVocabBC.value.contains(word.trim)) {     //添加正向词
          if (posWordMap.get(word).nonEmpty) posWordMap.put(word, (sentence, count, posWordMap(word)._2 + count))
          else posWordMap.put(word, (sentence, count, count))
        } else if (negVocabBC.value.contains(word.trim)) {    //添加负向词
          if (negWordMap.get(word).nonEmpty) negWordMap.put(word, (sentence, count, negWordMap(word)._2 + count))
          else negWordMap.put(word, (sentence, count, count))
        }
      }

      if (posWordMap.nonEmpty || negWordMap.nonEmpty) Some((scid, (posWordMap.toList, negWordMap.toList))) else None
    }

    import spark.implicits._
    val tmpData = transformedData.reduceByKey{(a, b) =>
      val posWordList: List[(String, (String, Long, Long))] = a._1 ::: b._1
      val negWordList: List[(String, (String, Long, Long))] = a._2 ::: b._2
      (posWordList, negWordList)
    }.map{record =>
      val scid: String = record._1
      val posWordList: List[(String, (String, Long, Long))] = record._2._1
      val negWordList: List[(String, (String, Long, Long))] = record._2._2

      val posWordMap = new mutable.HashMap[String, ((String, Long), Long)]()
      val negWordMap = new mutable.HashMap[String, ((String, Long), Long)]()

      posWordList.foreach{item =>
        if(posWordMap.get(item._1).nonEmpty) {
          val (pair, sum) = posWordMap(item._1)
          val maxPair = if(item._2._2 > pair._2) (item._2._1, item._2._2) else pair
          posWordMap.put(item._1, (maxPair, sum + item._2._3))
        } else posWordMap.put(item._1, ((item._2._1, item._2._2), item._2._3))
      }

      negWordList.foreach{item =>
        if(negWordMap.get(item._1).nonEmpty) {
          val (pair, sum) = negWordMap(item._1)
          val maxPair = if(item._2._2 > pair._2) (item._2._1, item._2._2) else pair
          negWordMap.put(item._1, (maxPair, sum + item._2._3))
        } else negWordMap.put(item._1, ((item._2._1, item._2._2), item._2._3))
      }

      val posList: List[(String, ((String, Long), Long))] = posWordMap.toList.sortBy(-_._2._2).take(5)
      val negList: List[(String, ((String, Long), Long))] = negWordMap.toList.sortBy(-_._2._2).take(5)

      val word_pos_top5: String = posList.map(a => a._1 + "," + a._2._2).mkString(";")
      val word_neg_top5: String = negList.map(a => a._1 + "," + a._2._2).mkString(";")
      val lyric_pos_top5: String = posList.map(a => a._2._1._1 + "@COUNT@" + a._2._1._2).mkString("@LYRIC@")
      val lyric_neg_top5: String = negList.map(a => a._2._1._1 + "@COUNT@" + a._2._1._2).mkString("@LYRIC@")

      (scid, word_pos_top5, word_neg_top5, lyric_pos_top5, lyric_neg_top5)
    }.toDF("scid", "word_pos_top5", "word_neg_top5", "lyric_pos_top5", "lyric_neg_top5")

    tmpData.createOrReplaceTempView("tmpTable")

    val createSQL: String = "create table if not exists mllab.lyric_for_emotion_new( " +
      "scid                string COMMENT '词条分类ID', " +
      "word_pos_top5       string COMMENT '正向前5个词及词频', " +
      "word_neg_top5       string COMMENT '负向前5个词及词频', " +
      "lyric_pos_top5      string COMMENT '正向词对应歌词', " +
      "lyric_neg_top5      string COMMENT '负向词对应歌词' " +
      ")ROW FORMAT DELIMITED  " +
      "FIELDS TERMINATED BY '|'  " +
      "STORED AS TEXTFILE "

    val insertSQL: String = "insert overwrite table mllab.lyric_for_emotion_new select * from tmpTable"

    spark.sql(createSQL)
    spark.sql(insertSQL)

    spark.stop()
  }
}
