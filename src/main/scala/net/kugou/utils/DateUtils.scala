package net.kugou.utils

import java.text.SimpleDateFormat
import java.util.Date

import org.joda.time.DateTime

/**
  * Author: yhao
  * Time: 2018/04/08 14:23
  * Desc: 时间转换工具类
  *
  */
class DateUtils extends Serializable {

  /**
    * 计算两个时间戳相隔天数
    *
    * @param millis1  时间戳1
    * @param millis2  时间戳2
    * @return 相隔天数
    */
  def dayInterval(millis1: Long, millis2: Long): Long = {
    val time1: DateTime = new DateTime(millis1)
    val time2: DateTime = new DateTime(millis2)

    // 获取当天开始的时间戳
    val millisStart1: Long = millis1 - time1.millisOfDay().get()
    val millisStart2: Long = millis2 - time2.millisOfDay().get()

    val days: Long = (millisStart1 - millisStart2) / (1000 * 60 * 60 * 24)

    days
  }


  /**
    * 计算两个日期相隔天数
    *
    * @param date1  日期1
    * @param date2  日期2
    * @return 相隔天数
    */
  def dayInterval(date1: Date, date2: Date): Long = {
    val millis1: Long = date1.getTime
    val millis2: Long = date2.getTime

    dayInterval(millis1, millis2)
  }


  /**
    * 计算两个日期相隔天数
    *
    * @param dateStr1  日期字符串1
    * @param dateStr2  日期字符串2
    * @param pattern   格式
    * @return 相隔天数
    */
  def dayInterval(dateStr1: String, dateStr2: String, pattern: String = "yyyy-MM-dd"): Long = {
    val format: SimpleDateFormat = new SimpleDateFormat(pattern)
    val date1: Date = format.parse(dateStr1)
    val date2: Date = format.parse(dateStr2)

    dayInterval(date1, date2)
  }


  /**
    * 将日期字符串从一种格式转换为另一种格式
    *
    * @param dateStr    日期字符串
    * @param inputPattern   输入格式
    * @param outputPattern  输出格式
    * @return
    */
  def dateTransform(dateStr: String, inputPattern: String, outputPattern: String): String = {
    val inputFormat: SimpleDateFormat = new SimpleDateFormat(inputPattern)
    val outputFormat: SimpleDateFormat = new SimpleDateFormat(outputPattern)

    val date: Date = inputFormat.parse(dateStr)
    outputFormat.format(date)
  }
}


object DateUtils extends Serializable {

  def apply(): DateUtils = new DateUtils()
}