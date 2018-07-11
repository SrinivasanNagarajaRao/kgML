package net.kugou.applications.undergraduatePredict

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.Some

/**
  * Author: yhao
  * Time: 2018/06/07 11:28
  * Desc: 通过大学生用户统计大学生最常安装的APP
  *
  */
object appStatByStudent extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val userSQL: String = "select userid from mllab.stu_appjwdstu_new2 where type='appjwdstu'"
    val appSQL: String = "select userid, app_list from dsl.dwf_userinfo_digging_3month_d " +
      "where dt='2018-04-30' and app_list is not null and length(trim(app_list))>1"

    val userData: DataFrame = spark.sql(userSQL)
    val appData: DataFrame = spark.sql(appSQL)
    val data: DataFrame = userData.join(appData, "userid")

    import spark.implicits._
    val studentAPPs: DataFrame = data.rdd.flatMap{row =>
      val appStr: String = row.getAs[String]("app_list")
      appStr.split(";").map{token => (token.split(",")(0), 1L)}
    }.reduceByKey(_ + _).sortBy(_._2, ascending = false).toDF("appname", "stu_cnt")

    val normalAPPs: DataFrame = appData.rdd.flatMap{row =>
      val appStr: String = row.getAs[String]("app_list")
      appStr.split(";").filter(_.nonEmpty).map{token =>
        val appTokens: Array[String] = token.split(",")
        if (appTokens.nonEmpty) (token.split(",")(0), 1L) else ("", 1L)
      }
    }.reduceByKey(_ + _).sortBy(_._2, ascending = false).toDF("appname", "cnt")

    val apps: DataFrame = studentAPPs.join(normalAPPs, "appname").selectExpr("appname", "stu_cnt", "cnt", "(stu_cnt+1)/(cnt+1) as rate")

    apps.createOrReplaceTempView("tmpTable")

    spark.sql("use temp")
    spark.sql("CREATE TABLE temp.stu_app_count_tmp AS select appname, stu_cnt, cnt, rate from tmpTable")

    spark.stop()
  }
}
