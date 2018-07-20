package net.kugou.utils.distance

/**
  * Author: yhao
  * Time: 2018/07/18 11:52
  * Desc: 两点间距离计算，包括：欧氏距离、余弦距离、Jaccard系数、Pearson相关系数
  *
  */
object Distances extends Serializable {

  /**
    * 使用Jaccard系数计算两set的相似度
    *
    * @param setA set A
    * @param setB set B
    * @return similarity
    */
  def jaccard(setA: Set[String], setB: Set[String]): Double = {
    val setC: Set[String] = setA & setB
    val setD: Set[String] = setA ++ setB
    1.0 * setC.size / setD.size
  }


  /**
    * 计算两数组的欧氏距离，如果数组长度不相等，返回0
    *
    * @param arrayA 数组A
    * @param arrayB 数组B
    * @return similarity
    */
  def euclidean(arrayA: Array[Double], arrayB: Array[Double]): Double = {
    if (arrayA.length == arrayB.length) {
      val sum: Double = arrayA.zip(arrayB).map(pair => Math.pow(pair._1 - pair._2, 2)).sum
      Math.sqrt(sum)
    } else 0
  }


  /**
    * 计算两数组的余弦距离，如果数组长度不相等，返回0
    *
    * @param arrayA 数组A
    * @param arrayB 数组B
    * @return similarity
    */
  def cosine(arrayA: Array[Double], arrayB: Array[Double]): Double = {
    if (arrayA.length == arrayB.length) {
      val sumA: Double = arrayA.map(item => item * item).sum
      val sumB: Double = arrayB.map(item => item * item).sum
      val sumC: Double = arrayA.zip(arrayB).map(pair => pair._1 * pair._2).sum

      sumC / (Math.sqrt(sumA) * Math.sqrt(sumB))
    } else 0
  }


  /**
    * 计算两数组的Pearson相关系数，如果数组长度不相等，返回0
    * 公式：(sum(xi*yi) - sum(xi)*sum(yi)/n) / (sqrt(sum(xi*xi) - sum(xi)*sum(xi)/n) * sqrt(sum(yi*yi) - sum(yi)*sum(yi)/n))
    *
    * @param arrayA 数组A
    * @param arrayB 数组B
    * @return similarity
    */
  def pearson(arrayA: Array[Double], arrayB: Array[Double]): Double = {
    if (arrayA.length == arrayB.length) {
      val size: Int = arrayA.length

      val sumA: Double = arrayA.sum
      val sumB: Double = arrayB.sum
      val sumAB: Double = arrayA.zip(arrayB).map(pair => pair._1 * pair._2).sum

      val SquareSumA: Double = arrayA.map(item => item * item).sum
      val SquareSumB: Double = arrayB.map(item => item * item).sum

      (sumAB - sumA * sumB / size) / (Math.sqrt(SquareSumA - sumA * sumA / size) * Math.sqrt(SquareSumB - sumB * sumB / size))
    } else 0
  }
}
