package demo.distance

import net.kugou.utils.distance.{GPSDistCalculator, GPSPoint}
import org.junit.Test

/**
  * Author: yhao
  * Time: 2018/06/21 14:48
  * Desc: 
  *
  */
class DistanceSuits {

  @Test
  def GPSDistDemo(): Unit = {
    val point1: GPSPoint = new GPSPoint(119.857661,49.207188)
    val point2: GPSPoint = new GPSPoint(119.811141,49.212795)

    val dist: Double = GPSDistCalculator.getDistance(point1, point2)
    println(dist)

    val startTime: Long = System.currentTimeMillis()
    var count: Int = 0
    while (count < 100000000) {
      GPSDistCalculator.getDistance(point1, point2)
      count += 1
    }
    val endTime: Long = System.currentTimeMillis()

    println(endTime - startTime)
  }


  @Test
  def GPSDistSimpleDemo(): Unit = {
    val point1: GPSPoint = new GPSPoint(119.857661,49.207188)
    val point2: GPSPoint = new GPSPoint(119.811141,49.212795)

    val dist: Double = GPSDistCalculator.getSimpleDistance(point1, point2)
    println(dist)

    val startTime: Long = System.currentTimeMillis()
    var count: Int = 0
    while (count < 100000000) {
      GPSDistCalculator.getSimpleDistance(point1, point2)
      count += 1
    }
    val endTime: Long = System.currentTimeMillis()

    println(endTime - startTime)
  }
}
