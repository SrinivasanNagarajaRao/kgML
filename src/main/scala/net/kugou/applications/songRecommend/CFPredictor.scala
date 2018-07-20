package net.kugou.applications.songRecommend

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.ml.recommendation.{ALSModel, TopByKeyAggregator}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.BoundedPriorityQueue

/**
  * Author: yhao
  * Time: 2018/07/12 14:59
  * Desc: 添加ALS预测代码，从Spark[2.3.1]的ALS.scala文件移植
  *
  */
class CFPredictor(spark: SparkSession, userData: DataFrame = null) extends Serializable {

  private def recommendForAll(
                               srcFactors: DataFrame,
                               dstFactors: DataFrame,
                               srcOutputColumn: String,
                               dstOutputColumn: String,
                               rank: Int,
                               num: Int): DataFrame = {
    import srcFactors.sparkSession.implicits._

    val srcFactorsBlocked = blockify(srcFactors.as[(Int, Array[Float])])
    val dstFactorsBlocked = blockify(dstFactors.as[(Int, Array[Float])])
    val ratings = srcFactorsBlocked.crossJoin(dstFactorsBlocked)
      .as[(Seq[(Int, Array[Float])], Seq[(Int, Array[Float])])]
      .flatMap { case (srcIter, dstIter) =>
        val m = srcIter.size
        val n = math.min(dstIter.size, num)
        val output = new Array[(Int, Int, Float)](m * n)
        var i = 0
        val pq = new BoundedPriorityQueue[(Int, Float)](num)(Ordering.by(_._2))
        srcIter.foreach { case (srcId, srcFactor) =>
          dstIter.foreach { case (dstId, dstFactor) =>
            // We use F2jBLAS which is faster than a call to native BLAS for vector dot product
            val score = blas.sdot(rank, srcFactor, 1, dstFactor, 1)
            pq += dstId -> score
          }
          pq.foreach { case (dstId, score) =>
            output(i) = (srcId, dstId, score)
            i += 1
          }
          pq.clear()
        }
        output.toSeq
      }
    // We'll force the IDs to be Int. Unfortunately this converts IDs to Int in the output.
    val topKAggregator = new TopByKeyAggregator[Int, Int, Float](num, Ordering.by(_._2))
    val recs = ratings.as[(Int, Int, Float)].groupByKey(_._1).agg(topKAggregator.toColumn).toDF("id", "recommendations")

    /*val arrayType = ArrayType(
      new StructType()
        .add(dstOutputColumn, IntegerType)
        .add("rating", FloatType)
    )
    recs.select($"id".as(srcOutputColumn), $"recommendations".cast(arrayType))*/

    recs.select($"id".as(srcOutputColumn), $"recommendations")
  }


  def recommendForAllUsers(model: ALSModel, numItems: Int, userMaxItems: Int = 1000): DataFrame = {
    val userFactors: DataFrame = model.userFactors
    val itemFactors: DataFrame = model.itemFactors
    val userCol: String = model.getUserCol
    val itemCol: String = model.getItemCol
    val rank: Int = model.rank

    val result: DataFrame = recommendForAll(userFactors, itemFactors, userCol, itemCol, rank, numItems + userMaxItems)

    if (Option(userData).nonEmpty) {
      import spark.implicits._
      result.join(userData, userCol).rdd.map{ row =>
        val userid: Int = row.getInt(0)
        var ratings: List[(Int, Float)] = row.getAs[Array[(Int, Float)]](1).toList
        val scids: Set[Int] = row.getAs[Set[Int]](2)
        ratings = ratings.filter(pair => !scids.contains(pair._1)).sortBy(-_._2).take(numItems)

        val ratingsStr: String = ratings.map(pair => pair._1 + "," + pair._2).mkString(";")
        (userid, ratingsStr)
      }.toDF(userCol, "recommendations")
    } else result
  }


  def recommendForUserSubset(model: ALSModel, dataset: Dataset[_], numItems: Int, userMaxItems: Int = 1000): DataFrame = {
    val userFactors: DataFrame = model.userFactors
    val itemFactors: DataFrame = model.itemFactors
    val userCol: String = model.getUserCol
    val itemCol: String = model.getItemCol
    val rank: Int = model.rank

    val srcFactorSubset = getSourceFactorSubset(dataset, userFactors, userCol)
    val result: DataFrame = recommendForAll(srcFactorSubset, itemFactors, userCol, itemCol, rank, numItems)

    if (Option(userData).nonEmpty) {
      import spark.implicits._
      result.join(userData, userCol).rdd.map{ row =>
        val userid: Int = row.getInt(0)
        var ratings: List[(Int, Float)] = row.getAs[Array[(Int, Float)]](1).toList
        val scids: Set[Int] = row.getAs[Set[Int]](2)
        ratings = ratings.filter(pair => !scids.contains(pair._1)).sortBy(-_._2).take(numItems)

        val ratingsStr: String = ratings.map(pair => pair._1 + "," + pair._2).mkString(";")
        (userid, ratingsStr)
      }.toDF(userCol, "recommendations")
    } else result
  }


  def recommendForAllItems(model: ALSModel, numUsers: Int): DataFrame = {
    val userFactors: DataFrame = model.userFactors
    val itemFactors: DataFrame = model.itemFactors
    val userCol: String = model.getUserCol
    val itemCol: String = model.getItemCol
    val rank: Int = model.rank

    recommendForAll(itemFactors, userFactors, itemCol, userCol, rank, numUsers)
  }


  def recommendForItemSubset(model: ALSModel, dataset: Dataset[_], numUsers: Int): DataFrame = {
    val userFactors: DataFrame = model.userFactors
    val itemFactors: DataFrame = model.itemFactors
    val userCol: String = model.getUserCol
    val itemCol: String = model.getItemCol
    val rank: Int = model.rank

    val srcFactorSubset = getSourceFactorSubset(dataset, itemFactors, itemCol)
    recommendForAll(srcFactorSubset, userFactors, itemCol, userCol, rank, numUsers)
  }


  private def getSourceFactorSubset(
                                     dataset: Dataset[_],
                                     factors: DataFrame,
                                     column: String): DataFrame = {
    factors
      .join(dataset.select(column), factors("id") === dataset(column), joinType = "left_semi")
      .select(factors("id"), factors("features"))
  }


  /**
    * Blockifies factors to improve the efficiency of cross join
    * TODO: SPARK-20443 - expose blockSize as a param?
    */
  private def blockify(
                        factors: Dataset[(Int, Array[Float])],
                        blockSize: Int = 4096): Dataset[Seq[(Int, Array[Float])]] = {
    import factors.sparkSession.implicits._
    factors.mapPartitions(_.grouped(blockSize))
  }
}
