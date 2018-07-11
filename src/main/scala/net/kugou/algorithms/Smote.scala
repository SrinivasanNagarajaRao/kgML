package net.kugou.algorithms

import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, BucketedRandomProjectionLSHModel}
import org.apache.spark.ml.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  *
  * Created by yhao on 2017/10/31 14:19.
  */
object Smote extends Serializable {

  /**
    * 过采样，同类别下局部随机选取样本进行采样
    *
    * @param data   待采样数据
    * @param featureCol   特征列列名
    * @param labelCol   标签列列明
    * @return   采样后数据
    */
  def overSampleByLocalRandom(data: DataFrame, featureCol: String, labelCol: String): DataFrame = {
    val spark: SparkSession = data.sparkSession

    val groupedData = data.select(featureCol, labelCol).rdd
    .map{case Row(feature: Vector, label: Double) => (label, feature)}
    .groupBy(_._1).cache()

    //对每个分类统计样本数
    var labelCountMap: collection.Map[Double, Int] = groupedData
      .map{record => (record._1, record._2.size)}
      .collectAsMap()

    //去掉样本数最大的分类
    val maxLabelCount: Int = labelCountMap.values.max
    labelCountMap = labelCountMap.filter(_._2 != maxLabelCount)

    //计算每个类别应达到的最大样本量
    val labelExpectMap: collection.Map[Double, Int] = labelCountMap.map{record =>
      val label: Double = record._1
      val expectCount: Int = ((0.3 * Math.random() + 0.7) * maxLabelCount).toInt
      (label, expectCount)
    }

    import spark.implicits._
    val resultData: DataFrame = groupedData.flatMap{record =>
      val label: Double = record._1
      val recordIter: Iterable[(Double, Vector)] = record._2
      var itemList: List[Vector] = recordIter.map(_._2).toList

      val expectCount: Int = labelExpectMap(label)
      if (labelExpectMap.keySet.contains(label)) {
        var resultList: List[Vector] = Nil

        for (_ <- Range(0, expectCount - itemList.size + 1)) {
          val indexA: Int = Math.floor(Math.random() * itemList.size).toInt
          val indexB: Int = Math.floor(Math.random() * itemList.size).toInt

          val vecA: Vector = itemList(indexA)
          val vecB: Vector = itemList(indexB)
          val itemPairs: Array[(Double, Double)] = vecA.toArray.zip(vecB.toArray)
          val result: Array[Double] = itemPairs.map{pair => pair._1 + Math.random() * (pair._2 - pair._1)}    //smote核心计算公式

          //TODO 此处生成的向量需要校准，结合Tomek link过滤掉生成向量V的k近邻中大部分为非本类向量的V
          val resultVec: SparseVector = Vectors.dense(result).toSparse
          resultList :+= resultVec
        }

        itemList ++= resultList
      }

      itemList.map(vec => (vec, label))
    }.toDF(featureCol, labelCol)

    resultData
  }


  /**
    * 过采样，基于LSH局部敏感哈希 + somte过采样算法，进行数据采样，LSH用于最近邻查找
    *
    * @param data   待采样数据
    * @param featureCol   特征列列名
    * @param labelCol   标签列列名
    * @param k    最近邻阈值
    * @return   采样后数据
    */
  def overSampleByLSHAndSmote(data: DataFrame, featureCol: String, labelCol: String, k: Int): DataFrame = {
    val spark: SparkSession = data.sparkSession

    val groupedData = data.select(featureCol, labelCol).rdd.map{case Row(feature: Vector, label: Double) => (label, feature)}.groupBy(_._1)

    //对每个分类统计样本数
    var labelCountMap: collection.Map[Double, Int] = groupedData.map{record => (record._1, record._2.size)}.collectAsMap()

    //去掉样本数最大的分类
    val maxLabelCount: Int = labelCountMap.values.max
    labelCountMap = labelCountMap.filter(_._2 != maxLabelCount)

    //计算最大类别与其他类别的比率
    val labelRateMap: collection.Map[Double, Int] = labelCountMap.map{record =>
      val label: Double = record._1
      val rate: Int = maxLabelCount / record._2
      (label, rate)
    }

    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(1000)
      .setNumHashTables(800)
      .setInputCol(featureCol)
      .setOutputCol("values")
    val model: BucketedRandomProjectionLSHModel = brp.fit(data)
    val transformdData: DataFrame = model.transform(data)

    val brpRDD: RDD[(Double, Vector, Vector)] = model.approxSimilarityJoin(transformdData, transformdData, 2.5)
      .filter("datasetA.userid != datasetB.userid")
      .filter(s"datasetA.$labelCol = datasetB.$labelCol")
      .select(s"datasetA.$labelCol", "datasetA.values", "datasetB.values").rdd
      .map{case Row(label: Double, vecA: Vector, vecB: Vector) => (label, vecA, vecB)}

    println("\n最近邻结果样例：")
    brpRDD.take(10).foreach(println)

    val resultRDD: RDD[(Vector, Double)] = brpRDD.groupBy(record => (record._1, record._2))
      .flatMap{record =>
        val label: Double = record._1._1
        val keyVec: Vector = record._1._2
        var resultList: List[(Vector, Double)] = Nil

        if (labelRateMap(label) > 1) {
          //从当前类别中采样k条记录
          val sampleList: List[Vector] = record._2.toList.map(_._3).take(k)

          //从采样后的list中根据比率随机采样
          var vecList: List[Vector] = Nil
          for(_ <- Range(0, labelRateMap(label) - 1)) {
            val index: Int = (Math.random() * sampleList.size).toInt
            vecList :+= sampleList(index)
          }

          val tmpResultList: List[Vector] = computeSomte(keyVec, vecList, labelRateMap.getOrElse(label, 1))

          resultList = tmpResultList.map(item => (item, label))
        }

        resultList
      }

    import spark.implicits._
    val result: DataFrame = resultRDD.toDF(featureCol, labelCol)

    data.select(featureCol, labelCol).union(result)
  }


  /**
    * 过采样，根据单条记录向量分量进行随机扰动（stochastic disturbance）生成新样本
    *
    * @param data   待采样数据
    * @param featureCol   特征列列名
    * @param labelCol   标签列列名
    * @param dbRate    随机扰动阈值，即设置扰动范围为：±(分量值 * 扰动阈值)
    * @return   采样后数据
    */
  def overSampleBySingleSD(data: DataFrame, featureCol: String, labelCol: String, dbRate: Double = 0.05): DataFrame = {
    val spark: SparkSession = data.sparkSession

    val dataRDD: RDD[(Double, Vector)] = data.select(featureCol, labelCol).rdd.map{case Row(feature: Vector, label: Double) => (label, feature)}

    //对每个分类统计样本数
    val labelCountMap: collection.Map[Double, Long] = dataRDD.map(record => (record._1, 1L)).reduceByKey(_ + _).collectAsMap()
    val maxLabelCount: Long = labelCountMap.values.max

    //计算最大类别与其他类别的比率
    val labelRateMap: collection.Map[Double, Long] = labelCountMap.map{record =>
      val label: Double = record._1
      val rate: Long = Math.round(maxLabelCount.toDouble / record._2)
      (label, rate)
    }

    println("\n个类别样本比率：")
    labelRateMap.foreach(println)

    val resultRDD: RDD[(Double, Vector)] = dataRDD.flatMap{record =>
      val label: Double = record._1
      var rate: Long = labelRateMap(label)
      var result: List[(Double, Vector)] = List(record)

      while (rate > 1) {
        val newArray: Array[Double] = record._2.toArray.map(element => element + (2 * Math.random() - 1) * dbRate * element)
        val newVector: Vector = Vectors.dense(newArray).toSparse
        result :+= (label, newVector)
        rate -= 1
      }

      result
    }

    import spark.implicits._
    resultRDD.toDF(labelCol, featureCol)
  }


  /**
    * 根据倍率multiple，从最近邻向量组中随机挑选multiple个向量与目标向量共同生成新向量
    * @param vecA   目标向量
    * @param vectors  最近邻向量组
    * @param multiple 倍率
    * @return 生成的向量组
    */
  private def computeSomte(vecA: Vector, vectors: List[Vector], multiple: Int): List[Vector] = {
    var resultList: List[Vector] = Nil

    var count: Int = multiple
    while (count > 1) {
      val index: Int = (Math.random() * vectors.size).toInt
      val vecB: Vector = vectors(index)
      val itemPairs: Array[(Double, Double)] = vecA.toArray.zip(vecB.toArray)
      val result: Array[Double] = itemPairs.map{pair => pair._1 + Math.random() * (pair._2 - pair._1)}    //smote核心计算公式
      val resultVec: SparseVector = Vectors.dense(result).toSparse
      resultList :+= resultVec
      count -= 1
    }

    resultList
  }
}
