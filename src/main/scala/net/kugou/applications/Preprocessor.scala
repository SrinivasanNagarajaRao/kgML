package net.kugou.applications

import net.kugou.pipeline.{KgTransformer, Segmenter}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType}

import scala.collection.mutable
import scala.reflect.io.File

/**
  *
  * Created by yhao on 2017/12/26 15:45.
  */
class Preprocessor extends Serializable {

  /**
    * 对姓名字段进行，去掉姓氏
    * @param data 待处理数据
    * @param stopwordPath   停用词文件路径，用于分词
    * @return   过滤姓名字段后数据
    */
  def filterName(data: DataFrame, stopwordPath: String): DataFrame = {
    val spark: SparkSession = data.sparkSession

    //分词
    val segmenter: Segmenter = new Segmenter()
      .setInputCol("useralias")
      .setOutputCol("seg_useralias")
      .setSegType("NShortSegment")
      .isDelNum(true)
      .isAddNature(true)
    val segedData: DataFrame = segmenter.transform(data)

    //姓名去掉姓氏
    val nameFilter = new KgTransformer[Seq[String], Seq[String]](new ArrayType(StringType, true), segName)
      .setInputCol(segmenter.getOutputCol)
      .setOutputCol("split_useralias")
    val splitedData: DataFrame = nameFilter.transform(segedData)

    //去除停用词
    val stopwords: Array[String] = spark.sparkContext.textFile(stopwordPath).collect()
    val remover: StopWordsRemover = new StopWordsRemover()
      .setInputCol(nameFilter.getOutputCol)
      .setOutputCol("filter_useralias")
      .setStopWords(stopwords)
    val removedData: DataFrame = remover.transform(splitedData)

    removedData
  }


  /**
    * 对给定的字段进行向量化，并assemble为一个特征字段
    * @param data   待处理数据
    * @param fields   需要进行向量化的字段名Array
    * @param vocabSizeMap   各字段向量化维度Map
    * @param modelPath    向量化模型保存路径
    * @return   assemble后的数据
    */
  def assemble(data: DataFrame, fields: Array[String], vocabSizeMap: mutable.HashMap[String, Int], modelPath: String): DataFrame = {
    var assembleFieldNames: List[String] = List()

    var tmpData: DataFrame = data
    for (field <- fields) {
      var vocabSize: Int = if(field.equals("filter_useralias")) vocabSizeMap("useralias") else vocabSizeMap(field)
      if (Option(vocabSize).isEmpty || vocabSize == 0) vocabSize = 10000
      val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol(field).setOutputCol("vec" + field).setVocabSize(vocabSize).fit(tmpData)
      tmpData = cvModel.transform(tmpData)
      cvModel.write.overwrite().save(modelPath + File.separator + s"${field}_cvModel")   //保存cv模型
      println(s"$field 向量维度：${cvModel.vocabulary.length}")

      val idfModel: IDFModel = new IDF().setInputCol(cvModel.getOutputCol).setOutputCol("idf" + field).fit(tmpData)
      tmpData = idfModel.transform(tmpData)
      idfModel.write.overwrite().save(modelPath + File.separator + s"${field}_idfModel")   //保存idf模型

      assembleFieldNames :+= idfModel.getOutputCol
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
    * 根据给定模型路径，加载向量化模型，对给定的字段进行向量化，并assemble为一个特征字段
    * @param data   待处理数据
    * @param fields   需要进行向量化的字段名Array
    * @param modelPath    向量化模型保存路径
    * @return   assemble后的数据
    */
  def assemble(data: DataFrame, fields: Array[String], modelPath: String): DataFrame = {
    var assembleFieldNames: List[String] = List()

    var tmpData: DataFrame = data
    for (field <- fields) {
      val cvModel: CountVectorizerModel = CountVectorizerModel.load(modelPath + File.separator + s"${field}_cvModel")
      tmpData = cvModel.transform(tmpData)
      cvModel.write.overwrite().save(modelPath + File.separator + s"${field}_cvModel")   //保存cv模型

      val idfModel: IDFModel = IDFModel.load(modelPath + File.separator + s"${field}_idfModel")
      tmpData = idfModel.transform(tmpData)
      idfModel.write.overwrite().save(modelPath + File.separator + s"${field}_idfModel")   //保存idf模型

      assembleFieldNames :+= idfModel.getOutputCol
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
    * 过滤有效数据，对指定向量化字段和指定阈值，只保留向量分量非零数大于阈值的数据
    * @param data   待处理数据
    * @param featureCol   要处理的向量化字段名
    * @param minNum   过滤阈值，非零分量数小于该值的向量将被过滤
    * @return   过滤后的数据
    */
  def filterEffectiveData(data: DataFrame, featureCol: String, minNum: Int): DataFrame = {
    println(s"\n开始对 $featureCol 特征过滤有效数据...")
    val effectiveData: DataFrame = data.filter{row =>
      val featureArray: Array[Double] = row.getAs[Vector](featureCol).toArray
      val sum: Int = featureArray.map(feature => if(feature != 0) 1 else 0).sum
      sum >= minNum
    }
    effectiveData
  }


  /**
    * 对姓名进行切分，去掉姓(第一个字)
    * @param tokens   切分后带词性的词Seq
    * @return   去除姓氏后的词Seq
    */
  private def segName(tokens: Seq[String]): Seq[String] = {
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
}
