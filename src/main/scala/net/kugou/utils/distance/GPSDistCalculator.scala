package net.kugou.utils.distance

import org.apache.log4j.Logger

/**
  * Author: yhao
  * Time: 2018/06/21 11:51
  * Desc: 
  *
  */
object GPSDistCalculator extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  private val  EARTHRADIUS = 6370996.81

  /**
    * 使用距离计算公式计算量经纬度之间的距离
    * @param lng1 经度1
    * @param lat1 纬度1
    * @param lng2 经度2
    * @param lat2 纬度2
    * @return
    */
  def getDistance(lng1: Double, lat1: Double, lng2: Double, lat2: Double): Double = {
    val point1_lng = _getLoop(lng1, -180, 180)
    val point1_lat = _getRange(lat1, -74, 74)
    val point2_lng = _getLoop(lng2, -180, 180)
    val point2_lat = _getRange(lat2, -74, 74)

    val x1 = degreeToRad(point1_lng)
    val y1 = degreeToRad(point1_lat)
    val x2 = degreeToRad(point2_lng)
    val y2 = degreeToRad(point2_lat)

    EARTHRADIUS * Math.acos(Math.sin(y1) * Math.sin(y2) + Math.cos(y1) * Math.cos(y2) * Math.cos(x2 - x1))
  }


  /**
    * 简化的经纬度间距离计算公式，使用多项式计算代替三角计算，以减小计算量，适用于纬度20~46度范围
    * @param lng1 经度1
    * @param lat1 纬度1
    * @param lng2 经度2
    * @param lat2 纬度2
    * @return
    */
  def getSimpleDistance(lng1: Double, lat1: Double, lng2: Double, lat2: Double): Double = {
    val dX: Double = lng1 - lng2
    val dY: Double = lat1 - lat2
    val avgY: Double = 0.5 * (lat1 + lat2)

    val distX: Double = (0.05 * Math.pow(avgY, 3) - 19.16 * Math.pow(avgY, 2) + 47.13 * avgY + 110966 ) * dX
    val distY: Double = (17 * avgY + 110352) * dY

    Math.sqrt(Math.pow(distX, 2) + Math.pow(distY, 2))
  }


  def getDistance(point1: GPSPoint, point2: GPSPoint): Double = {
    this.getDistance(point1.lng, point1.lat, point2.lng, point2.lat)
  }


  def getSimpleDistance(point1: GPSPoint, point2: GPSPoint): Double = {
    this.getSimpleDistance(point1.lng, point1.lat, point2.lng, point2.lat)
  }


  /**
    * 将角度制转换为弧度制
    *
    * @param degree 角度
    * @return 对应的弧度
    */
  def degreeToRad(degree: Double): Double = Math.PI * degree / 180


  /**
    * 将v规范到(a, b)之间
    * @param v  待处理的对象
    * @param a  区间下限
    * @param b  区间上限
    * @return
    */
  private def _getLoop(v: Double, a: Double, b: Double): Double = {
    var n=v
    while( n > b) n -= b - a
    while(n < a) n += b - a

    n
  }


  /**
    * 将v规范到(a, b)之间
    * @param v  待处理的对象
    * @param a  区间下限
    * @param b  区间上限
    * @return
    */
  private def _getRange(v: Double, a: Double, b: Double): Double = {
    val tem: Double = if (v > a) v else a
    if(tem > b) b else tem
  }
}
