package net.kugou.applications.songRecommend

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

/**
  * Author: yhao
  * Time: 2018/07/18 14:39
  * Desc: 
  *
  */
object RecoByUserCF extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val checkpointPath = "hdfs:/user/haoyan/checkpoint"
    val spark: SparkSession = SparkUtils().createSparkEnv(checkPointDir = checkpointPath)

    //    val sql: String = s"select userid, scid, rating from temp.tb_user_item_rating_2000"
//    val origData: DataFrame = spark.sql(sql)
    val sql: String = s"select userid, scid, rating from temp.tb_user_item_rating_20w_new where rating>10"
    val topSongSQL: String = "select cast(scid as int) scid from dsl.dwm_list_play_scid_d where dt='2018-07-17' and pt='android' and audio_play>800 group by scid"
    val origData: DataFrame = spark.sql(sql).join(spark.sql(topSongSQL), "scid")

    val data: RDD[(Int, List[(Int, Double)])] = origData.rdd.map{row =>
      val scid: Int = row.getInt(0)
      val userid: Int = row.getInt(1)
      val rating: Double = row.getDouble(2)
      (userid, List((scid, rating)))
    }.reduceByKey(_ ::: _).filter(_._2.size>=5)
    println("加载数据条数：" + data.count())

    //过滤，只取每个用户评分最高的topN歌曲
    val ratingsRDD: RDD[(Int, (Int, Int))] = data.flatMap {record =>
      val userid: Int = record._1
      val scidList: List[(Int, Double)] = record._2.sortBy(-_._2).take(100)
      scidList.map(item => (item._1, (userid, scidList.size)))
    }

    val simiUserData = computeSimiUserNew(spark, ratingsRDD, 50)

    //-- 评分数据与新歌join
    val newSongSQL: String = "select cast(scid as int) from mllab.wjh_newsong_temp"
    val newSongData: DataFrame = spark.sql(newSongSQL)

    import spark.implicits._
    val scidData = origData.rdd.map{row =>
      val scid: Int = row.getInt(0)
      val userid: Int = row.getInt(1)
      val rating: Double = row.getDouble(2)
      (userid, List((scid, rating)))
    }.reduceByKey(_ ::: _).toDF("userid", "scidlist")
    //---

    val joinedData: DataFrame = simiUserData.join(scidData, simiUserData("simiUser")===scidData("userid"))

    val result: DataFrame = joinedData.rdd.map{ row =>
      val userid: Int = row.getInt(0)
      val simiUser: Int = row.getInt(1)
      val similarity: Double = row.getDouble(2)
      val scidList: List[(Int, Double)] = row.getAs[mutable.WrappedArray[Row]]("scidlist").map{row => (row.getInt(0), row.getDouble(1))}.toList
      val weightedScidList: List[(Int, Double)] = scidList.map(item => (item._1, Math.log(item._2) * similarity))

      (userid, weightedScidList)
    }.reduceByKey(_ ::: _).map{record =>
      val userid: Int = record._1
      val scidList: List[(Int, Double)] = record._2.sortBy(-_._2)
      val scidMap: mutable.LinkedHashMap[Int, Double] = new mutable.LinkedHashMap[Int, Double]()
      for (pair <- scidList if scidMap.get(pair._1).isEmpty) scidMap.put(pair._1, pair._2)
      val scidStr: String = scidMap.take(100).toList.map(item => item._1 + "_" + item._2).mkString(";")
      (userid, scidStr)
    }.toDF("userid", "scidStr")

    result.createOrReplaceTempView("resultTable")
    spark.sql("use temp")
    spark.sql("create table temp.tb_user_item_userCF_20w_result as select userid, scidStr from resultTable")

    spark.stop()
  }


  def computeSimiUserNew(spark: SparkSession, data: RDD[(Int, (Int, Int))], simiUserNum: Int): DataFrame = {
    val ratingPairs = data.join(data).filter{f =>
      val ratio: Double = 1.0 * f._2._1._2 / f._2._2._2
      (f._2._1._1<f._2._2._1) && (ratio>0.1 && ratio<10)
    }

    val joinedData = ratingPairs.map{record =>
      val userA: Int = record._2._1._1
      val userB: Int = record._2._2._1
      ((userA, userB), (record._2._1._2, record._2._2._2, 1))
    }
    println("用户pair数量：" + joinedData.count())

    import spark.implicits._
    val ratingData = joinedData.reduceByKey{(item1, item2) =>
      val cntA: Int = Math.max(item1._1, item2._1)
      val cntB: Int = Math.max(item1._2, item2._2)
      val cntAB: Int = item1._3 + item2._3
      (cntA, cntB, cntAB)
    }.map{record =>
      val (userA, userB) = record._1
      val similarity: Double = 1.0 * record._2._3 / (record._2._1 + record._2._2 - record._2._3)
      (userA, (userB, similarity))
    }

    val reversedRatingData: RDD[(Int, (Int, Double))] = ratingData.map(item => (item._2._1, (item._1, item._2._2)))

    val unionData = ratingData.union(reversedRatingData).map{record =>
      (record._1, List(record._2))
    }.reduceByKey(_ ::: _).map{record =>
      val userid: Int = record._1
      (userid, record._2.sortBy(-_._2).take(simiUserNum))
    }

    unionData.toDF("userid", "simiUsers").createOrReplaceTempView("tmpTable")
    spark.sql("use temp")
    spark.sql("create table temp.tb_userCF_simiUsers as select userid, simiUsers from tmpTable")

    unionData.flatMap{record =>
      record._2.map(item => (record._1, item._1, item._2))
    }.toDF("userid", "simiUser", "similarity")
  }


  def computeSimiUser(spark: SparkSession, data: RDD[(Int, (Int, List[(Int, Double)], Int))], simiUserNum: Int): DataFrame = {
    val ratingPairs = data.join(data).filter(f => f._2._1._1<f._2._2._1)
    val userRatings: RDD[(Int, Int, Double)] = ratingPairs.flatMap{record =>
      val scidSizeA: Int = record._2._1._3
      val scidSizeB: Int = record._2._2._3
      val ratio: Double = 1.0 * scidSizeA / scidSizeB

      if (ratio>0.1 && ratio<10) {    //如果A，B歌曲数相差10倍以上，则不计算相似度
        val (userA, userB) = (record._2._1._1, record._2._2._1)
        val scidSetA: Set[Int] = record._2._1._2.map(_._1).toSet
        val scidSetB: Set[Int] = record._2._2._2.map(_._1).toSet

        val interSize: Int = (scidSetA & scidSetB).size
        val similarity: Double = 1.0 * interSize / (scidSizeA + scidSizeB - interSize)

        Some((userA, userB, similarity))
      } else None
    }

    val userRatingsReverse: RDD[(Int, Int, Double)] = userRatings.map(record => (record._2, record._1, record._3))

    import spark.implicits._
    userRatings.union(userRatingsReverse).map{record =>
      (record._1, List((record._2, record._3)))
    }.reduceByKey(_ ::: _).flatMap{record =>
      val userid: Int = record._1
      record._2.sortBy(-_._2).take(simiUserNum).map(item => (userid, item._1, item._2))
    }.toDF("userid", "simiUser", "similarity")
  }


  def computeSimiUserOld(spark: SparkSession, data: DataFrame, simiUserNum: Int): DataFrame = {
    //--- 统计用户pair包含的scid
    import spark.implicits._
    val pairData = data.rdd.map{row =>
      val userid: Int = row.getInt(0)
      val scid: Int = row.getInt(1)
      (scid, List(userid))
    }.reduceByKey(_ ::: _).flatMap{record =>
      val scid: Int = record._1
      val userList: List[Int] = record._2.sorted
      var pairList: List[((Int, Int), Set[Int])] = Nil

      for (i <- userList.indices; j <- i + 1 until userList.size) {
        pairList :+= ((userList(i), userList(j)), Set(scid))
      }

      pairList
    }.reduceByKey(_ ++ _).map{record =>
      (record._1._1, record._1._2, record._2.size)
    }.toDF("useridA", "useridB", "interSize")
    println("用户对数量：" + pairData.count())


    //--- 统计用户包含的scid
    val userData = data.rdd.map{ row =>
      val userid: Int = row.getInt(0)
      val scid: Int = row.getInt(1)
      (userid, Set(scid))
    }.reduceByKey(_ ++ _).map{record => (record._1, record._2.toList)}.toDF("userid", "scidList")

    val joinedA = pairData.join(userData, pairData("useridA")===userData("userid"), "left_outer").drop("userid").withColumnRenamed("scidList", "scidListA#")
    val joinedB = joinedA.join(userData, joinedA("useridB")===userData("userid"), "left_outer").drop("userid").withColumnRenamed("scidList", "scidListB#")

    val simiUserData = joinedB.rdd.flatMap {row =>
      val useridA: Int = row.getInt(0)
      val useridB: Int = row.getInt(1)
      val interSize = row.getInt(2)

      val scidSetA: Set[Int] = row.getAs[mutable.WrappedArray[Int]](3).toSet
      val scidSetB: Set[Int] = row.getAs[mutable.WrappedArray[Int]](4).toSet
      val unionSet: Set[Int] = scidSetA ++ scidSetB

      val rating: Double = 1.0 * interSize / unionSet.size

      List((useridA, List((useridB, rating)))) ++ List((useridB, List((useridA, rating))))
    }.reduceByKey(_ ::: _).map{record =>
      val sampleList: List[(Int, Double)] = record._2.sortBy(-_._2).take(simiUserNum)
      (record._1, sampleList)
    }

    //--- 存储相似用户
    val tmpData = simiUserData.map{record =>
      val userid: Int = record._1
      val userStr: String = record._2.map(item => item._1 + "," + item._2).mkString(";")
      (userid, userStr)
    }.toDF("userid", "simiUsers")

    tmpData.createOrReplaceTempView("tmpTable")
    spark.sql("use temp")
    spark.sql("create table temp.simi_user_for_userCF_20w as select userid, simiUsers from tmpTable")
    //---

    simiUserData.flatMap{ record =>
      record._2.map(item=> (record._1, item._1, item._2))
    }.toDF("userid", "simiUser", "similarity")
  }


  def blockify(
                        factors: Dataset[(Int, List[Int], Int)],
                        blockSize: Int = 4096): Dataset[Seq[(Int, List[Int], Int)]] = {
    import factors.sparkSession.implicits._
    factors.mapPartitions(_.grouped(blockSize))
  }
}
