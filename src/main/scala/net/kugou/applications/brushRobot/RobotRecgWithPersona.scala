package net.kugou.applications.brushRobot

import net.kugou.utils.{DateUtils, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.matching.Regex

/**
  * Author: yhao
  * Time: 2018/04/04 17:06
  * Desc: 
  *
  */
object RobotRecgWithPersona {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()


//    val date: String = "2018-03-20"
//    val month: String = "2018-02"
//    val personaDTable: String = "dal.ua_bi_persona_new_d"
//    val personaMTable: String = "dal.ua_bi_persona_new_m"
//    val k: Int = 200
//    val resultTable: String = "mllab.tb_yh_robot_recg_kmeans_d"
//    val centerPath: String = "hdfs://kgdc/user/haoyan/result/robot/persona"

    val date: String = args(0)
    val month: String = args(1)
    val personaDTable: String = args(2)
    val personaMTable: String = args(3)
    val k: Int = args(4).toInt
    val resultTable: String = args(5)
    val centerPath: String = args(6)

    val dayInfoSQL: String = "select cast(userid as string) userid, cnt_start, cnt_login, cnt_play_audio, dur_play_audio, " +
      "cnt_play_audio_radio, cnt_play_audio_search, cnt_play_audio_favorite, cnt_play_audio_list, " +
      "cnt_favorite_scid, cnt_download_audio, cnt_favorite_list, cnt_share, cnt_comment, " +
      s"is_auto_pay, search_cnt, search_kw_cnt from $personaDTable where dt='$date'"

    val monInfoSQL: String = "select trim(userid), trim(nickname), trim(phone_num), trim(province), usr_friend, " +
      "usr_follow, usr_fan, reg_time, time_interval, mon_active, mon_silence, day_start, day_login, day_active, " +
      "day_play_audio, is_pay, last_pay_time " +
      s"from $personaMTable where dm='$month'"

    println("当日数据查询SQL: " + dayInfoSQL)
    println("上月数据查询SQL: " + monInfoSQL)

    val dailyData: DataFrame = loadDailyData(spark, dayInfoSQL)
    val monthData: DataFrame = loadMonthData(spark, monInfoSQL, date)

    val data: DataFrame = dailyData.join(monthData, Seq("userid"), "left_outer").na.fill(0)

    val featureCols = Array("cntStart", "cntLogin", "cntPlayAudio", "durPlayAudio", "cntPlayAudioRadio",
      "cntPlayAudioSearch", "cntPlayAudioFavorite", "cntPlayAudioList", "cntFavoriteScid", "cntDownloadAudio",
      "cntFavoriteList", "cntShare", "cntComment", "isAutoPay", "searchCnt", "searchKwCnt",
      "isNicknameZH", "isNicknameNUM", "hasPhone", "provinceNum", "friendNum", "followNum", "fansNum",
      "regInterval", "monActive", "monSilence", "dayStartup", "dayLogin", "dayActive",
      "dayPlayAudio", "isPay", "isPay30")

    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    val assembledData: DataFrame = assembler.transform(data)

    val kMeans: KMeans = new KMeans()
      .setMaxIter(200)
      .setTol(1e-6)
      .setK(k)
    val model: KMeansModel = kMeans.fit(assembledData)

    val predictions = model.transform(assembledData)

    // 计算模型的均方误差
    val WSSSE = model.computeCost(assembledData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // 打印聚类中心点，并保存
    println("Cluster Centers: ")
    val centers: Array[(Int, linalg.Vector)] = model.clusterCenters.zipWithIndex.map(_.swap)
    val centersRDD: RDD[(Int, linalg.Vector)] = spark.sparkContext.parallelize(centers)
    centers.foreach(println)

    import spark.implicits._
    centersRDD.map{pair =>
      pair._1 + ":" + pair._2.toArray.mkString(",")
    }.toDF("centers").write.format("json").mode("overwrite").save(centerPath)

    // 打印结果样例
    println("Result Samples: ")
    predictions.select("userid", "prediction", "features").show(20, truncate = false)

    val createSQL: String = s"CREATE TABLE IF NOT EXISTS $resultTable(" +
      "userid                     string," +
      "cluster                    int," +
      "cntStart                   double," +
      "cntLogin                   double," +
      "cntPlayAudio               double," +
      "durPlayAudio               double," +
      "cntPlayAudioRadio          double," +
      "cntPlayAudioSearch         double," +
      "cntPlayAudioFavorite       double," +
      "cntPlayAudioList           double," +
      "cntFavoriteScid            double," +
      "cntDownloadAudio           double," +
      "cntFavoriteList            double," +
      "cntShare                   double," +
      "cntComment                 double," +
      "isAutoPay                  double," +
      "searchCnt                  double," +
      "searchKwCnt                double," +
      "isNicknameZH               double," +
      "isNicknameNUM              double," +
      "hasPhone                   double," +
      "provinceNum                double," +
      "friendNum                  double," +
      "followNum                  double," +
      "fansNum                    double," +
      "regInterval                double," +
      "monActive                  double," +
      "monSilence                 double," +
      "dayStartup                 double," +
      "dayLogin                   double," +
      "dayActive                  double," +
      "dayPlayAudio               double," +
      "isPay                      double," +
      "isPay30                    double" +
      ")partitioned by (dt string) " +
      "ROW FORMAT DELIMITED " +
      "FIELDS TERMINATED BY '|' " +
      "STORED AS TEXTFILE"

    val insertSQL: String = s"INSERT OVERWRITE TABLE $resultTable partition(dt) " +
      "select userid, prediction as cluster, cntStart, cntLogin, cntPlayAudio, " +
      "durPlayAudio, cntPlayAudioRadio, cntPlayAudioSearch, cntPlayAudioFavorite, " +
      "cntPlayAudioList, cntFavoriteScid, cntDownloadAudio, cntFavoriteList, " +
      "cntShare, cntComment, isAutoPay, searchCnt, searchKwCnt, isNicknameZH, " +
      "isNicknameNUM, hasPhone, provinceNum, friendNum, followNum, fansNum, " +
      "regInterval, monActive, monSilence, dayStartup, dayLogin, dayActive, " +
      s"dayPlayAudio, isPay, isPay30, '$date' as dt from tmpTable"

    // 保存结果到hive表
    predictions.select("userid", "prediction", "cntStart", "cntLogin", "cntPlayAudio",
      "durPlayAudio", "cntPlayAudioRadio", "cntPlayAudioSearch", "cntPlayAudioFavorite",
      "cntPlayAudioList", "cntFavoriteScid", "cntDownloadAudio", "cntFavoriteList",
      "cntShare", "cntComment", "isAutoPay", "searchCnt", "searchKwCnt", "isNicknameZH",
      "isNicknameNUM", "hasPhone", "provinceNum", "friendNum", "followNum", "fansNum",
      "regInterval", "monActive", "monSilence", "dayStartup", "dayLogin", "dayActive",
      "dayPlayAudio", "isPay", "isPay30"
    ).createOrReplaceTempView("tmpTable")

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql(createSQL)
    spark.sql(insertSQL)

    spark.stop()
  }


  def loadDailyData(spark: SparkSession, sql: String): DataFrame = {
    val data: DataFrame = spark.sql(sql)

    import spark.implicits._
    val result = data.rdd.map{row =>
      val userid: String = row.getString(0)
      val cntStart: Int = if(Option(row.get(1)).isEmpty) 0 else row.getInt(1)
      val cntLogin: Int = if(Option(row.get(2)).isEmpty) 0 else row.getInt(2)
      val cntPlayAudio: Int = if(Option(row.get(3)).isEmpty) 0 else row.getInt(3)
      val durPlayAudio: Int = if(Option(row.get(4)).isEmpty) 0 else row.getInt(4)
      val cntPlayAudioRadio: Int = if(Option(row.get(5)).isEmpty) 0 else row.getInt(5)
      val cntPlayAudioSearch: Int = if(Option(row.get(6)).isEmpty) 0 else row.getInt(6)
      val cntPlayAudioFavorite: Int = if(Option(row.get(7)).isEmpty) 0 else row.getInt(7)
      val cntPlayAudioList: Int = if(Option(row.get(8)).isEmpty) 0 else row.getInt(8)
      val cntFavoriteScid: Int = if(Option(row.get(9)).isEmpty) 0 else row.getInt(9)
      val cntDownloadAudio: Int = if(Option(row.get(10)).isEmpty) 0 else row.getInt(10)
      val cntFavoriteList: Int = if(Option(row.get(11)).isEmpty) 0 else row.getInt(11)
      val cntShare: Int = if(Option(row.get(12)).isEmpty) 0 else row.getInt(12)
      val cntComment: Int = if(Option(row.get(13)).isEmpty) 0 else row.getInt(13)
      val isAutoPay: Int = if(Option(row.get(14)).isEmpty) 0 else row.getByte(14).toInt
      val searchCnt: Int = if(Option(row.get(15)).isEmpty) 0 else row.getInt(15)
      val searchKwCnt: Int = if(Option(row.get(16)).isEmpty) 0 else row.getInt(16)

      (userid, cntStart.toDouble, cntLogin.toDouble, cntPlayAudio.toDouble, durPlayAudio.toDouble,
        cntPlayAudioRadio.toDouble, cntPlayAudioSearch.toDouble, cntPlayAudioFavorite.toDouble,
        cntPlayAudioList.toDouble, cntFavoriteScid.toDouble, cntDownloadAudio.toDouble, cntFavoriteList.toDouble,
        cntShare.toDouble, cntComment.toDouble, isAutoPay.toDouble, searchCnt.toDouble, searchKwCnt.toDouble)
    }.toDF("userid", "cntStart", "cntLogin", "cntPlayAudio", "durPlayAudio", "cntPlayAudioRadio", "cntPlayAudioSearch",
      "cntPlayAudioFavorite", "cntPlayAudioList", "cntFavoriteScid", "cntDownloadAudio", "cntFavoriteList", "cntShare",
      "cntComment", "isAutoPay", "searchCnt", "searchKwCnt")

    result
  }


  def loadMonthData(spark: SparkSession, sql: String, date: String): DataFrame = {
    // 省份映射表
    val provinceMap: Map[String, Int] = Map("安徽" -> 1, "北京" -> 2, "福建" -> 3, "甘肃" -> 4, "广东" -> 5, "广西" -> 6,
      "贵州" -> 7, "海南" -> 8, "河北" -> 9, "河南" -> 10, "黑龙江" -> 11, "湖北" -> 12, "湖南" -> 13, "吉林" -> 14,
      "江苏" -> 15, "江西" -> 16, "辽宁" -> 17, "内蒙古" -> 18, "宁夏" -> 19, "青海" -> 20, "山东" -> 21, "山西" -> 22,
      "陕西" -> 23, "上海" -> 24, "四川" -> 25, "台湾" -> 26, "天津" -> 27, "西藏" -> 28, "香港" -> 29, "新疆" -> 30,
      "云南" -> 31, "浙江" -> 32, "重庆" -> 33, "澳门" -> 34)

    val data: DataFrame = spark.sql(sql)

    import spark.implicits._
    val result = data.rdd.mapPartitions{iter =>
      val dateUtils: DateUtils = new DateUtils

      iter.map{row =>
        val userid: String = row.getString(0)
        val nickname: String = if(Option(row.get(1)).isEmpty) "" else row.getString(1)
        val hasPhone: Int = if(Option(row.get(2)).isEmpty) 0 else 1
        val province: String = if(Option(row.get(3)).isEmpty) "" else row.getString(3)
        val friendNum: Int = if(Option(row.get(4)).isEmpty) 0 else row.getInt(4)
        val followNum: Int = if(Option(row.get(5)).isEmpty) 0 else row.getInt(5)
        val fansNum: Int = if(Option(row.get(6)).isEmpty) 0 else row.getInt(6)
        val regTime: String = if(Option(row.get(7)).isEmpty) "" else row.getInt(7).toString
        val timeInterval: String = if(Option(row.get(8)).isEmpty) "" else row.getString(8)
        val monActive: Int = if(Option(row.get(9)).isEmpty) 0 else row.getInt(9)
        val monSilence: Int = if(Option(row.get(10)).isEmpty) 0 else row.getInt(10)
        val dayStartup: Int = if(Option(row.get(11)).isEmpty) 0 else row.getByte(11).toInt
        val dayLogin: Int = if(Option(row.get(12)).isEmpty) 0 else row.getByte(12).toInt
        val dayActive: Int = if(Option(row.get(13)).isEmpty) 0 else row.getByte(13).toInt
        val dayPlayAudio: Int = if(Option(row.get(14)).isEmpty) 0 else row.getByte(14).toInt
        val isPay: Int = if(Option(row.get(15)).isEmpty) 0 else row.getByte(15).toInt
        val lastPayTime: String = if(Option(row.get(16)).isEmpty) "" else row.getInt(16).toString

        val regexZH: Regex = new Regex("[\u4e00-\u9fa5]")
        val regexNUM: Regex = new Regex("\\d+")

        val isNicknameZH: Int = if (nickname.nonEmpty && regexZH.findFirstIn(nickname).nonEmpty) 1 else 0
        val isNicknameNUM: Int = if (nickname.nonEmpty && regexNUM.findFirstIn(nickname).nonEmpty) 1 else 0

        // 获取省份对应索引号
        val provinceSet: Set[String] = provinceMap.keySet
        val provinceNum: Int = if (province.nonEmpty && provinceSet.contains(province)) provinceMap(province) else 0

        // 获取注册日期到date的天数
        var regInterval: Long = 0
        if (regTime.length > 7) {
          val dateTransformed: String = dateUtils.dateTransform(regTime, "yyyyMMdd", "yyyy-MM-dd")
          regInterval = dateUtils.dayInterval(date, dateTransformed) + 1
        }

        // 时间段偏好列表
//        val timeFavorList: Seq[String] = if (timeInterval.nonEmpty) timeInterval.split(",").toSeq else Seq()

        // 最近30天是否有付费行为
        var isPay30: Int = 0
        if (lastPayTime.nonEmpty) {
          val dateTransformed: String = dateUtils.dateTransform(lastPayTime, "yyyyMMdd", "yyyy-MM-dd")
          val payInterval: Long = dateUtils.dayInterval(date, dateTransformed) + 1
          if (payInterval < 30) isPay30 = 1
        }

        (userid, isNicknameZH.toDouble, isNicknameNUM.toDouble, hasPhone.toDouble, provinceNum.toDouble,
          friendNum.toDouble, followNum.toDouble, fansNum.toDouble, regInterval.toDouble, /*timeFavorList,*/
          monActive.toDouble, monSilence.toDouble, dayStartup.toDouble, dayLogin.toDouble, dayActive.toDouble,
          dayPlayAudio.toDouble, isPay.toDouble, isPay30.toDouble)
      }
    }.toDF("userid", "isNicknameZH", "isNicknameNUM", "hasPhone", "provinceNum", "friendNum", "followNum", "fansNum",
      "regInterval", /*"timeFavorList", */"monActive", "monSilence", "dayStartup", "dayLogin", "dayActive",
      "dayPlayAudio", "isPay", "isPay30")

    result
  }
}
