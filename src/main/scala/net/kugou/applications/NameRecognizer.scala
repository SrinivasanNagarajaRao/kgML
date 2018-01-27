package net.kugou.applications

import net.kugou.utils.{DataTypeUtils, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 基于用户通讯录关系，使用NLP方法提取用户真实姓名
  *
  * Created by yhao on 2017/12/28 17:44.
  */
object NameRecognizer extends Serializable {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val relationTable: String = ""
    val relationDate: String = "2017-12-30"

    val relationSQL: String = s"select detail from $relationTable where dt='$relationDate' and detail is not null"
    val relationData: DataFrame = spark.sql(relationSQL)

    relationData.rdd.flatMap{row =>
      val tokens: Array[String] = row.getString(0).split(";")
      val pairs: Array[(String, List[String])] = tokens.flatMap{token =>
        val items: Array[String] = token.split(",")
        if (items.length > 1) {
          val userid: String = items(1)
          var tmpName: String = items(0).trim.replaceAll("\"", "")
          if (userid.length > 1 && tmpName.nonEmpty) {
            tmpName = if (tmpName.contains("u") && !tmpName.contains("\\")) tmpName.substring(tmpName.indexOf("u"), tmpName.length) else tmpName
            val uName = if(tmpName.contains("\\") || tmpName.matches("\\d+")) tmpName
            else DataTypeUtils().addSplit(tmpName.trim, 5, "\\")
            val name: String = DataTypeUtils().unicodeToString(uName)
            Some((userid, List(name)))
          } else None
        } else None
      }
      pairs
    }.reduceByKey(_ ::: _).map{record =>
      record._1

    }

    spark.stop()
  }
}
