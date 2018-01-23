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
}


object DataTypeUtils extends Serializable {
  def apply(): DataTypeUtils = new DataTypeUtils()
}