package net.kugou.utils

import java.util.regex.{Matcher, Pattern}

/**
  *
  * Created by yhao on 2017/12/11 12:42.
  */
class DataTypeUtils extends Serializable {

  /**
    * 将unicode转换为string
    *
    * @param str  待转换字符串
    * @return
    */
  def unicodeToString(str: String): String = {
    val pattern: Pattern = Pattern.compile("(\\\\u(\\p{XDigit}{4}))")
    val matcher: Matcher = pattern.matcher(str)

    var ch: Char = 0
    var result: String = str
    while (matcher.find()) {
      val group = matcher.group(2)
      ch = Integer.parseInt(group, 16).toChar
      val group1 = matcher.group(1)
      result = result.replace(group1, ch + "")
    }

    result
  }


  /**
    * 向字符串添加分隔符，在字符串开头添加分隔符，之后每隔interval步长添加一个分隔符
    *
    * @param str  待处理字符串
    * @param interval   步长
    * @param seg    分隔符
    * @return 添加分隔符的字符串
    */
  def addSplit(str: String, interval: Int, seg: String): String = {
    var tmpStr: String = str
    var result: String = ""
    var length: Int = tmpStr.length

    while (length > 0) {
      result += seg
      if(length > interval) {
        result += tmpStr.substring(0, interval)
        tmpStr = tmpStr.substring(interval, length)
      } else {
        result += tmpStr
        tmpStr = ""
      }
      length = tmpStr.length
    }

    result
  }
}


object DataTypeUtils extends Serializable {

  def apply(): DataTypeUtils = new DataTypeUtils()
}