package net.kugou.applications.sexPredict

import java.util.regex.{Matcher, Pattern}

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * Created by yhao on 2017/12/26 16:48.
  */
object GenTableForSexTrain extends Serializable {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val date: String = args(0)
    var dm: String = args(1)
    val diggingTable: String = args(2)
    val personasTable: String = args(3)
    val relationTable: String = args(4)
    val resultTable: String = args(5)

    if (dm.isEmpty) {if(date.length > 7) dm = date.substring(0, 7) else dm = date}

    //加载用户行为数据
    val diggingSQL: String = s"select userid, cast(sex as int) as sex, nickname, app_list, play_songid_list, '$dm' as dm, pt from $diggingTable " +
      s"where dt='$date' and sex in ('0', '1')"
    val personasSQL: String = s"select userid, top_singer from $personasTable where dm='$dm' and top_singer is not null"
    val diggingData: DataFrame = spark.sql(diggingSQL)
    val personasData: DataFrame = spark.sql(personasSQL)
    val userInfoData: DataFrame = diggingData.join(personasData, Seq("userid"), "left_outer")

    //加载用户姓名数据
    val relationSQL: String = s"select detail from $relationTable where dt='2017-11-20' and detail is not null"
    val relationData: DataFrame = spark.sql(relationSQL)
    val userNameData: DataFrame = getUserName(relationData)

    val trainData: DataFrame = userInfoData.join(userNameData, Seq("userid"), "left_outer").select(
      "userid", "sex", "nickname", "username", "useralias", "app_list", "play_songid_list", "top_singer", "dm", "pt")
    println("训练数据共：" + trainData.count())

    trainData.createOrReplaceTempView("tmpTable")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val createTableSQL: String = s"create table if not exists $resultTable(" +
      s"userid string COMMENT '用户ID', " +
      s"sex int COMMENT '用户性别', " +
      s"nickname string COMMENT '用户昵称', " +
      s"username string COMMENT '用户姓名', " +
      s"useralias string COMMENT '用户别名', " +
      s"app_list string COMMENT '安装app列表', " +
      s"play_songid_list string COMMENT '歌曲播放列表', " +
      s"top_singer string COMMENT '喜欢歌手top10' " +
      s")PARTITIONED BY (dm string, pt string) " +
      s"ROW FORMAT DELIMITED  " +
      s"FIELDS TERMINATED BY '|'  STORED AS TEXTFILE"

    val insertSQL: String = s"INSERT INTO $resultTable partition(dm, pt) select * from tmpTable"

    spark.sql(createTableSQL)
    spark.sql(insertSQL)

    spark.stop()
  }



  def getUserName(data: DataFrame): DataFrame = {
    val spark: SparkSession = data.sparkSession

    import spark.implicits._
    val nameData: DataFrame = data.rdd.flatMap{row =>
      val tokens: Array[String] = row.getString(0).split(";")
      val pairs: Array[(String, List[String])] = tokens.flatMap{token =>
        val items: Array[String] = token.split(",")
        if (items.length > 1) {
          val userid: String = items(1)
          var tmpName: String = items(0).trim.replaceAll("\"", "")
          if (userid.length > 1 && tmpName.nonEmpty) {
            tmpName = if (tmpName.contains("u") && !tmpName.contains("\\")) tmpName.substring(tmpName.indexOf("u"), tmpName.length) else tmpName
            val uName: String = if(tmpName.contains("\\") || tmpName.matches("\\d+")) tmpName
            else addSplit(tmpName.trim, 5, "\\")
            val name: String = unicodeToString(uName)
            Some((userid, List(name)))
          } else None
        } else None
      }
      pairs
    }.reduceByKey(_ ::: _).flatMap{record =>
      val userid: String = record._1
      val nameList: List[String] = record._2
      val userAlias: String = nameList.distinct.mkString(";")
      val userName: String = nameList.map(word => (word, 1L)).groupBy(_._1).mapValues(list => list.map(_._2).sum).maxBy(_._2)._1

      if(userid.nonEmpty) Some((userid, userName, userAlias)) else None
    }.toDF("userid", "username", "useralias")

    nameData
  }


  def addSplit(str: String, interval: Int, seg: String): String = {
    var tmpStr: String = str
    var result: String = ""
    var length: Int = tmpStr.length

    while (length > 0) {
      result += seg
      if(length > interval) {
        result += tmpStr.substring(0, interval)
        tmpStr = tmpStr.substring(interval, length)
      } else {
        result += tmpStr
        tmpStr = ""
      }
      length = tmpStr.length
    }

    result
  }


  def unicodeToString(str: String): String = {
    val pattern: Pattern = Pattern.compile("(\\\\u(\\p{XDigit}{4}))")
    val matcher: Matcher = pattern.matcher(str)

    var ch: Char = 0
    var result: String = str
    while (matcher.find()) {
      val group: String = matcher.group(2)
      ch = Integer.parseInt(group, 16).toChar
      val group1: String = matcher.group(1)
      result = result.replace(group1, ch + "")
    }

    result
  }
}
