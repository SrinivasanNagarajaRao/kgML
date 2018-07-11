package net.kugou.applications.workOnWeekend

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: yhao
  * Time: 2018/07/06 14:17
  * Desc: 从BI流水获取经纬度和地址，生成用户每日白天和晚上最常出现的地点和经纬度
  *
  */
object GenPermanentByDay extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val dt = args(0)
    val pt = args(1)
    val resultTable = args(2)

//    val dt = "2018-05-01"
//    val pt = "ios"
//    val resultTable = "mllab.tb_userid_perm_d"

    var sql: String = ""
    if (pt.equalsIgnoreCase("android")) {
      sql = "select i userid, lvt time, svar2 longitude, svar3 latitude, svar4 address " +
        s"from ddl.dt_list_ard_d where action='lbs' and dt='$dt' and cast(i as int)>0 and svar2 is not null " +
        s"and svar3 is not null and svar4 is not null and lvt is not null"
    } else if (pt.equalsIgnoreCase("ios")) {
      sql = "select i userid, lvt time, ivar2 longitude, ivar3 latitude, ivar4 address " +
        s"from ddl.dt_list_ios_d where action='trash' and dt='$dt' and cast(i as int)>0 and ivar2 is not null " +
        s"and ivar3 is not null and ivar4 is not null and lvt is not null"
    }

    println(s"SQL: $sql")

    import spark.implicits._
    val result: DataFrame = spark.sql(sql).rdd.flatMap{row =>
      val userid: String = row.getString(0)
      val time: String = row.getString(1)
      val longitude: Double = row.getString(2).toDouble
      val latitude: Double = row.getString(3).toDouble
      val address: String = row.getString(4)

      val hourStr: String = time.split("\\s+")(1).split(":")(0)

      val hour: Int = if (hourStr.matches("\\d+")) {
        if (hourStr.toInt>100) hourStr.toInt / 10 else hourStr.toInt
      } else -1

      if (longitude>0 && latitude>0 && hour>=0) {
        val dayTime: Int = if (hour>7 && hour<18) 1 else 0

        Some(((userid, dayTime, address), List((longitude, latitude))))
      } else None
    }.reduceByKey(_ ::: _, 1000).filter(_._2.size<500).map{record =>
      val (userid, dayTime, address) = record._1
      val jwdList: List[(Double, Double)] = record._2
      val size: Int = jwdList.size

      val (sumLng, sumLat) = jwdList.reduce{(a, b) => (a._1 + b._1, a._2 + b._2)}
      var avgLongitude: Double = sumLng / size
      var avgLatitude: Double = sumLat / size

      avgLongitude = 1.0 * (avgLongitude * 1000000).toInt / 1000000
      avgLatitude = 1.0 * (avgLatitude * 1000000).toInt / 1000000

      ((userid, dayTime), List((size, avgLongitude, avgLatitude, address)))
    }.reduceByKey(_ ::: _).map{record =>
      val (userid, dayTime) = record._1
      val (count, longitude, latitude, address) = record._2.maxBy(_._1)

      (userid, dayTime, count, longitude, latitude, address)
    }.toDF("userid", "dayTime", "count", "longitude", "latitude", "address")

    result.createOrReplaceTempView("tmpTable")

    val createSQL: String = s"create table if not exists $resultTable(" +
      "userid string COMMENT 'userid', " +
      "is_day int COMMENT '是否白天', " +
      "cnt int COMMENT '出现次数', " +
      "longitude double COMMENT '平均经度', " +
      "latitude double COMMENT '平均纬度', " +
      "address string COMMENT '具体地址'" +
      ")PARTITIONED BY (dt string, pt string) " +
      "ROW FORMAT DELIMITED " +
      "FIELDS TERMINATED BY '|' " +
      "STORED AS TEXTFILE"

    val insertSQL: String = s"insert overwrite table $resultTable partition(dt='$dt', pt='$pt') " +
      s"select userid, dayTime is_day, count cnt, longitude, latitude, address from tmpTable"

    spark.sql("set hive.mapred.mode=nonstrict")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql(createSQL)
    spark.sql(insertSQL)

    spark.stop()
  }
}
