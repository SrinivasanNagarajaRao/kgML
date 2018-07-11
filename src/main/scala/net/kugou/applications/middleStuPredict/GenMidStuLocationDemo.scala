package net.kugou.applications.middleStuPredict

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: yhao
  * Time: 2018/06/27 16:24
  * Desc: 根据用户位置信息进行过滤，过滤出中学的地址以及对应经纬度
  *
  */
object GenMidStuLocationDemo extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val startDate: String = "2018-04-01"
    val endDate: String = "2018-05-31"

    // 加载用户轨迹数据
    val ardSQL: String = "select userid, trail, 'ard' type, dt from (" +
      "select userid, trail, pmod(datediff(dt, '2012-01-01'), 7) weekday, dt " +
      s"from mllab.t_scid_user_Trail_d_ard where dt between '$startDate' and '$endDate' and trail is not null " +
      s") t1 where t1.weekday>0 and t1.weekday<6"

    val iosSQL: String = "select userid, trail, 'ios' type, dt from (" +
      "select userid, trail, pmod(datediff(dt, '2012-01-01'), 7) weekday, dt " +
      s"from mllab.t_scid_user_Trail_d_ios where dt between '$startDate' and '$endDate' and trail is not null " +
      s") t1 where t1.weekday>0 and t1.weekday<6"

    val ardData: DataFrame = spark.sql(ardSQL)
    val iosData: DataFrame = spark.sql(iosSQL)

    import spark.implicits._
    val result: DataFrame = ardData.union(iosData).rdd.flatMap{row =>
      val userid: String = row.getString(0)
      val pointList: Array[String] = row.getString(1).split("\\|")
      val srcType: String = row.getString(2)
      val dt: String = row.getString(3)

      val filterAddrList: List[(String, Double, Double, String, String)] = pointList.flatMap{item =>
        if (item.length > 2) {
          val tokens: Array[String] = item.substring(1, item.length - 1).split(",")
          if (tokens.length>3) {
            val time: String = tokens(0)
            val hour: Int = time.split(" ")(1).split(":")(0).toInt
            val longitude: Double = tokens(1).toDouble
            val latitude: Double = tokens(2).toDouble
            val address: String = tokens(3)

            var tmpAddr: String = address
            if (srcType.equalsIgnoreCase("ard") && address.contains("靠近")) tmpAddr = tmpAddr.substring(tmpAddr.indexOf("靠近"))

            //用于判断是否在7:00~17:59
            val timeFlag: Boolean = hour>=7 && hour<18

            //用于判断是否中学生
            val addrFlag: Boolean = tmpAddr.contains("中学") || tmpAddr.contains("高中") || tmpAddr.contains("初中") || tmpAddr.contains("中专")

            if (longitude>0 && latitude>0 && address.length>4 && timeFlag && addrFlag) Some((time, longitude, latitude, address, dt)) else None
          } else None
        } else None
      }.toList

      filterAddrList.map(record => (userid, record._1, record._2, record._3, record._4, record._5))
    }.toDF("userid", "time", "longitude", "latitude", "address", "dt")

    result.createOrReplaceTempView("tmpTable")

    spark.sql("set hive.mapred.mode=nonstrict")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("insert overwrite table mllab.stu_user_jwd_middle_d partition(dt) select userid, time, longitude, latitude, address, dt from tmpTable")

    spark.stop()
  }
}
