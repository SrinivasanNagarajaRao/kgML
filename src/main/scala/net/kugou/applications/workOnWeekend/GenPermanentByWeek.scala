package net.kugou.applications.workOnWeekend

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: yhao
  * Time: 2018/07/09 15:38
  * Desc: 
  *
  */
object GenPermanentByWeek extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val startDate: String = "2018-05-01"
    val endDate: String = "2018-05-31"
    val dm: String = startDate.substring(0, 7)
    val pt: String = "android"
    val resultTable: String = "mllab.tb_userid_perm_m"

    // 加载用户轨迹数据
    var sql: String = ""
    if (pt.equalsIgnoreCase("android")) {
      sql = "select userid, trail, pmod(datediff(dt, '2012-01-01'), 7) weekday, dt " +
        s"from mllab.t_scid_user_Trail_d_ard where dt between '$startDate' and '$endDate' and trail is not null "
    } else if (pt.equalsIgnoreCase("ios")) {
      sql = "select userid, trail, pmod(datediff(dt, '2012-01-01'), 7) weekday, dt " +
        s"from mllab.t_scid_user_Trail_d_ios where dt between '$startDate' and '$endDate' and trail is not null "
    }

    val data: DataFrame = spark.sql(sql)

    import spark.implicits._
    val result: DataFrame = data.rdd.flatMap{row =>
      val userid: String = row.getString(0)
      val pointList: Array[String] = row.getString(1).split("\\|")
      val weekDay: Int = row.getInt(2)
      val dt: String = row.getString(3)

      val addressList: List[(String, Boolean, Double, Double, String)] = pointList.flatMap{item =>
        if (item.length > 2) {
          val tokens: Array[String] = item.substring(1, item.length - 1).split(",")
          if (tokens.length>3) {
            val time: String = tokens(0)
            val hourStr: String = time.split("\\s+")(1).split(":")(0)
            val longitude: Double = tokens(1).toDouble
            val latitude: Double = tokens(2).toDouble
            val address: String = tokens(3)

            val hour: Int = if (hourStr.matches("\\d+")) {
              if (hourStr.toInt>100) hourStr.toInt / 10 else hourStr.toInt
            } else -1

            //用于判断是否在7:00~17:59
            val timeFlag: Boolean = hour>=7 && hour<18

            //用于判断地址是否以“市”、“区"、“县”、“镇”结尾，如果以它们结尾，则地址范围太大，应舍弃
            val lastChar: String = address.substring(address.length - 1, address.length)
            val charFlag: Boolean = lastChar == "市" || lastChar == "县" || lastChar == "区" || lastChar == "镇"

            if (longitude>0 && latitude>0 && hour>=0 && address.trim != "-1" && !charFlag) Some((time, timeFlag, longitude, latitude, address)) else None
          } else None
        } else None
      }.toList

      addressList.map(record => ((userid, weekDay, record._2, record._5), List((record._3, record._4))))
    }.reduceByKey(_ ::: _).filter(_._2.size<500).map{record =>
      val (userid, weekDay, timeFlag, address) = record._1
      val jwdList: List[(Double, Double)] = record._2
      val size: Int = jwdList.size

      val (sumLng, sumLat) = jwdList.reduce{(a, b) => (a._1 + b._1, a._2 + b._2)}
      var avgLongitude: Double = sumLng / size
      var avgLatitude: Double = sumLat / size

      avgLongitude = 1.0 * (avgLongitude * 1000000).toInt / 1000000
      avgLatitude = 1.0 * (avgLatitude * 1000000).toInt / 1000000

      ((userid, weekDay, timeFlag), List((size, avgLongitude, avgLatitude, address)))
    }.reduceByKey(_ ::: _).map{record =>
      val (userid, weekDay, timeFlag) = record._1
      val (count, longitude, latitude, address) = record._2.maxBy(_._1)

      (userid, weekDay, timeFlag, count, longitude, latitude, address)
    }.toDF("userid", "weekDay", "timeFlag", "count", "longitude", "latitude", "address")

    result.createOrReplaceTempView("tmpTable")

    val createSQL: String = s"create table if not exists $resultTable(" +
      "userid string COMMENT 'userid', " +
      "weekDay int COMMENT '星期几', " +
      "timeFlag int COMMENT '是否白天', " +
      "cnt int COMMENT '出现次数', " +
      "longitude double COMMENT '平均经度', " +
      "latitude double COMMENT '平均纬度', " +
      "address string COMMENT '具体地址'" +
      ")PARTITIONED BY (dm string, pt string) " +
      "ROW FORMAT DELIMITED " +
      "FIELDS TERMINATED BY '|' " +
      "STORED AS TEXTFILE"

    val insertSQL: String = s"insert overwrite table $resultTable partition(dm='$dm', pt='$pt') " +
      s"select userid, weekDay, timeFlag, count cnt, longitude, latitude, address from tmpTable"

    spark.sql("set hive.mapred.mode=nonstrict")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql(createSQL)
    spark.sql(insertSQL)

    spark.stop()
  }
}
