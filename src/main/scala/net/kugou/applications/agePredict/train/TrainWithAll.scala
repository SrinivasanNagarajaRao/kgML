package net.kugou.applications.agePredict.train

import net.kugou.pipeline.{KgTransformer, Segmenter}
import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.reflect.io.File

/**
  *
  * Created by yhao on 2017/12/11 14:44.
  */
object TrainWithAll extends Serializable {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val platform: String = args(0)
    val iterNum: Int = args(1).toInt
    val minFeatureNum: Int = args(2).toInt
    val label: String = args(3)
    val sourceTable: String = args(4)
    val resultTable: String = args(5)
    val stopwordPath: String = args(6)
    val modelPath: String = args(7)

    val spark: SparkSession = SparkUtils().createSparkEnv()

    val userid: String = "userid"
    val dm: String = "2017-09"

    //数值型字段
    val numericFields: Array[String] = Array("age_avg", "cnt_start", "cnt_login", "cnt_play_audio", "cnt_scid", "cnt_play_audio_lib",
      "cnt_play_audio_radio", "cnt_play_audio_search", "cnt_play_audio_favorite", "cnt_play_audio_list",
      "cnt_play_audio_like", "cnt_play_audio_recommend", "cnt_play_video", "cnt_download_audio", "cnt_search",
      "cnt_search_valid", "cnt_favorite_scid", "cnt_favorite_list", "cnt_share", "cnt_comment", "usr_follow", "usr_fan",
      "keep_pay_month", "his_musicpack_buy_cnt", "his_musicpack_free_cnt", "year_musicpack_free_cnt",
      "first_musicpack_buy_valid_days", "last_musicpack_buy_apart_days", "his_svip_buy_cnt", "last_svip_buy_valid_days",
      "single_buy_cnt", "year_album_buy_cnt", "year_album_buy_times", "deductpoint_cnt")

    //文本型字段
    var textFields: Array[String] = Array("nickname", "useralias")

    //列表型字段
    val listFields: Array[String] = Array("app_list", "play_songid_list", "top_singer")

    //构建SQL
    var sql: String = s"select $userid, cast($label as int) as age, pt "

    if (numericFields.nonEmpty) sql += s", ${numericFields.mkString(", ")} "
    if (textFields.nonEmpty) sql += s", ${textFields.mkString(", ")} "
    if (listFields.nonEmpty) sql += s", ${listFields.mkString(", ")} "

    sql += s" from $sourceTable where dm='$dm' and $label is not null "
//    if (listFields.nonEmpty) sql += listFields.map(field => " and " + field + " is not null").mkString(" ")
    if (listFields.nonEmpty) sql += listFields.flatMap{field =>
      if (!field.equals("app_list")) Some(" and " + field + " is not null") else None
    }.mkString(" ")

    if (!platform.equalsIgnoreCase("all")) sql += s" and pt='$platform'"

    //加载数据
    val data: DataFrame = loadData(spark, sql, numericFields, textFields, listFields)
    println(s"\n共加载数据：${data.count()}")
    println("样例数据：")
    data.show(10, truncate = false)

    //删除nickname字段(已与useralias字段合并)
    textFields = textFields.dropWhile(_.equals("nickname"))

    val (preData, vecFieldNames, vocabSizeMap) = preprocess(data, textFields, listFields, stopwordPath)
    val vecData: DataFrame = assemble(preData, numericFields, vecFieldNames, minFeatureNum, vocabSizeMap, modelPath)

    println(s"\n实际训练数据共：${vecData.count()} 条！")
    train(vecData, "age", "features", "predictions", iterNum, modelPath)

    spark.stop()
  }


  def loadData(spark: SparkSession, sql: String, numericFields: Array[String],
               textFields: Array[String], listFields: Array[String]): DataFrame = {
    println(s"Hive查询SQL: $sql")

    import spark.implicits._
    val result: DataFrame = spark.sql(sql).rdd.map{row =>
      val userid: String = if(Option(row.getString(0)).isEmpty) "" else row.getString(0)
      val age: Double = if(Option(row.get(1)).isEmpty) 7 else row.getInt(1).toDouble
      val pt: String = if(Option(row.getString(2)).isEmpty) "" else row.getString(2)

      //处理数值型字段
      var length: Int = 3 + numericFields.length
      var numericList: List[Double] = Nil
      for (i <- 3 until length) {
        val numericValue: Double = if(Option(row.get(i)).isEmpty) 0 else row.get(i).toString.toDouble
        numericList :+= numericValue
      }

      //处理文本型字段
      var textList: List[String] = Nil
      for (j <- length until (length + textFields.length)) {
        val textValue: String = if(Option(row.get(j)).isEmpty) "" else row.get(j).toString
        textList :+= textValue
      }

      //处理列表型字段
      length += textFields.length
      var listList: List[Seq[String]] = Nil
      for (k <- length until (length + listFields.length)) {
        val listValue: String = if (Option(row.get(k)).isEmpty) "" else row.get(k).toString

        listFields(k - length) match {
          case "play_songid_list" =>
            val songidSeq: Seq[String] = listValue.split(";").map(_.split(":")(0)).toSeq
            listList :+= songidSeq
          case "app_list" =>
            val appSeq: Seq[String] = listValue.split(";").map(_.split(",")(0)).toSeq
            listList :+= appSeq
          case "top_singer" =>
            val singerSeq: Seq[String] = listValue.split(",").toSeq
            listList :+= singerSeq
          case _ =>
            val commonSeq: Seq[String] = listValue.split(";").toSeq
            listList :+= commonSeq
        }
      }

      val useralias: String = if(textList(1).nonEmpty) textList(1) else textList.head      //如果用户姓名不为空，则使用用户姓名，否则使用昵称

      allFeaturesWithLabel(userid, age, pt, numericList.head, numericList(1), numericList(2), numericList(3), numericList(4),
        numericList(5), numericList(6), numericList(7), numericList(8), numericList(9), numericList(10), numericList(11),
        numericList(12), numericList(13), numericList(14), numericList(15), numericList(16), numericList(17), numericList(18),
        numericList(19), numericList(20), numericList(21), numericList(22), numericList(23), numericList(24), numericList(25),
        numericList(26), numericList(27), numericList(28), numericList(29), numericList(30), numericList(31), numericList(32),
        numericList(33), useralias, listList.head, listList(1), listList(2))
    }.toDF()

    /*println("欠采样前数据分布：")
    result.select("age").rdd.map(row => (row.getDouble(0), 1L)).reduceByKey(_ + _).collect().sortBy(_._1).foreach(println)

    val sampledResult: DataFrame = underSample(result, "age", 670000L)

    println("欠采样后数据分布：")
    sampledResult.select("age").rdd.map(row => (row.getDouble(0), 1L)).reduceByKey(_ + _).collect().sortBy(_._1).foreach(println)

    sampledResult*/

    result.repartition(2000)
  }


  /**
    * 对text和list类型数据进行预处理
    * @param data   待处理数据
    * @param textFields   text数据列名
    * @param listFields   list数据列名
    * @param stopwordPath   停用词路径
    * @return
    */
  def preprocess(data: DataFrame, textFields: Array[String], listFields: Array[String], stopwordPath: String) = {
    val spark: SparkSession = data.sparkSession

    var tmpData: DataFrame = data
    val stopwords: Array[String] = spark.sparkContext.textFile(stopwordPath).collect()
    var vecFieldNames: List[String] = listFields.toList     //需要向量化的特征

    //设置各特征向量维度
    val vocabSizeMap: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
    vocabSizeMap.put("app_list", 10000)
    vocabSizeMap.put("play_songid_list", 15000)
    vocabSizeMap.put("down_songid_list", 8000)
    vocabSizeMap.put("top_singer", 5000)
    vocabSizeMap.put("nickname", 20000)
    vocabSizeMap.put("useralias", 20000)
    vocabSizeMap.put("address", 30000)
    vocabSizeMap.put("search_kw_list", 5000)


    for (textField <- textFields) {
      val segmenter: Segmenter = new Segmenter().setInputCol(textField).setOutputCol("seg" + textField).setSegType("NShortSegment").isDelNum(true)
      segmenter.isAddNature(true)

      var segedData: DataFrame = segmenter.transform(tmpData)
      var outputCol: String = segmenter.getOutputCol

      if (textField.equals("nickname") || textField.equals("useralias")) {
        //定义对姓名进行切分的函数
        def segName(tokens: Seq[String]): Seq[String] = {
          val filterTokens: Seq[String] = tokens.flatMap{token =>
            val word: String = token.split("\\/")(0)
            val nature: String = token.split("\\/")(1)

            var result: Seq[String] = Seq(word)
            if (nature.equals("nr")) {
              val slice: String = word.drop(1)
              result = Seq(slice)
            }
            result
          }
          filterTokens.filter(_.trim.nonEmpty)
        }

        val nameFilter = new KgTransformer[Seq[String], Seq[String]](new ArrayType(StringType, true), segName)
          .setInputCol("seg" + textField)
          .setOutputCol("split" + textField)
        segedData = nameFilter.transform(segedData)
        outputCol = nameFilter.getOutputCol
      } else {
        segmenter.isAddNature(false)
        segedData = segmenter.transform(tmpData)
      }

      val remover: StopWordsRemover = new StopWordsRemover().setInputCol(outputCol).setOutputCol("filter" + textField).setStopWords(stopwords)
      tmpData = remover.transform(segedData)
      vecFieldNames :+= "filter" + textField
      vocabSizeMap.put("filter" + textField, vocabSizeMap(textField))
    }

    (tmpData, vecFieldNames, vocabSizeMap)
  }


  /**
    * 向量化特征，并进行assemble
    * @param data   待处理数据
    * @param numericFields    数值型特征
    * @param vecFieldNames    待向量化特征
    * @param minFeatureNum    最小有效特征数（向量化后有效特征数小于该值的向量将丢弃）
    * @param vocabSizeMap     向量化vocab大小
    * @param modelPath        模型保存路径
    * @return
    */
  def assemble(data: DataFrame, numericFields: Array[String], vecFieldNames: List[String],
               minFeatureNum: Int, vocabSizeMap: mutable.HashMap[String, Int], modelPath: String): DataFrame = {
    var assembleFieldNames: List[String] = numericFields.toList   //用于聚合的字段名
    var tmpData = data

    println("\n各list类型特征向量维度：")
    for (i <- vecFieldNames.indices) {
      val vecField: String = vecFieldNames(i)
      val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol(vecField).setOutputCol("vec" + vecField).setVocabSize(vocabSizeMap(vecField)).fit(tmpData)
      tmpData = cvModel.transform(tmpData)
      cvModel.write.overwrite().save(modelPath + File.separator + s"all_${vecField}_cvModel")   //保存cv模型

      //仅对applist和songid过滤有效特征
      val effectiveData: DataFrame = if (vecField.contains("app_list") || vecField.contains("play_songid_list")) {
        filterEffectiveData(tmpData, cvModel.getOutputCol, minFeatureNum)
      } else {
        filterEffectiveData(tmpData, cvModel.getOutputCol, 1)
      }
//      val effectiveData: DataFrame = tmpData

      val idfModel: IDFModel = new IDF().setInputCol(cvModel.getOutputCol).setOutputCol("idf" + vecField).fit(effectiveData)
      tmpData = idfModel.transform(effectiveData)
      idfModel.write.overwrite().save(modelPath + File.separator + s"all_${vecField}_idfModel")   //保存idf模型

      assembleFieldNames :+= idfModel.getOutputCol
      println(s"特征：$vecField, 向量维度：${vocabSizeMap(vecField)}")
    }

    //将各特征合并为一个特征向量
    println("\n开始进行向量assemble...")
    val assembler = new VectorAssembler()
      .setInputCols(assembleFieldNames.toArray)
      .setOutputCol("features")
    val vectorizedData: DataFrame = assembler.transform(tmpData)

    vectorizedData
  }


  /**
    * 训练模型
    * @param data   训练数据集
    * @param labelCol   label列名
    * @param featureCol   feature列名
    * @param predictionCol    预测结果列名
    * @param iterNum    最大迭代次数
    * @param modelPath    模型保存路径
    * @return
    */
  def train(data: DataFrame, labelCol: String, featureCol: String,
            predictionCol: String, iterNum: Int, modelPath: String): LogisticRegressionModel = {
    val lr: LogisticRegression = new LogisticRegression()
      .setLabelCol(labelCol)
      .setFeaturesCol(featureCol)
      .setPredictionCol(predictionCol)
      .setMaxIter(iterNum)
    val model: LogisticRegressionModel = lr.fit(data)
    model.write.overwrite().save(modelPath + File.separator + "all_lrModel")

    model
  }


  def filterEffectiveData(data: DataFrame, featureCol: String, minNum: Int): DataFrame = {
    println(s"\n开始对 $featureCol 特征过滤有效数据...")
    val effectiveData: DataFrame = data.filter{row =>
      val featureArray: Array[Double] = row.getAs[Vector](featureCol).toArray
      val sum: Int = featureArray.map(feature => if(feature != 0) 1 else 0).sum
      sum >= minNum
    }
    effectiveData
  }
}


case class allFeaturesWithLabel(userid: String, age: Double, pt: String, age_avg: Double, cnt_start: Double, cnt_login: Double, cnt_play_audio: Double,
                                cnt_scid: Double, cnt_play_audio_lib: Double, cnt_play_audio_radio: Double, cnt_play_audio_search: Double,
                                cnt_play_audio_favorite: Double, cnt_play_audio_list: Double, cnt_play_audio_like: Double,
                                cnt_play_audio_recommend: Double, cnt_play_video: Double, cnt_download_audio: Double, cnt_search: Double,
                                cnt_search_valid: Double, cnt_favorite_scid: Double, cnt_favorite_list: Double, cnt_share: Double,
                                cnt_comment: Double, usr_follow: Double, usr_fan: Double, keep_pay_month: Double,
                                his_musicpack_buy_cnt: Double, his_musicpack_free_cnt: Double, year_musicpack_free_cnt: Double,
                                first_musicpack_buy_valid_days: Double, last_musicpack_buy_apart_days: Double, his_svip_buy_cnt: Double,
                                last_svip_buy_valid_days: Double, single_buy_cnt: Double, year_album_buy_cnt: Double,
                                year_album_buy_times: Double, deductpoint_cnt: Double /*, age_avg: Double*/ , useralias: String, /*search_kw_list: String,*/
                                app_list: Seq[String], play_songid_list: Seq[String] /*, down_songid_list: Seq[String]*/ , top_singer: Seq[String])