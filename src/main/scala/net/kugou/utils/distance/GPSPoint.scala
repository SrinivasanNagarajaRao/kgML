package net.kugou.utils.distance

/**
  * Author: yhao
  * Time: 2018/06/21 14:26
  * Desc: 
  *
  */
class GPSPoint(
                val lng: Double,
                val lat: Double,
                val pointId: Long = 0L) extends Serializable {


  override def equals (that: Any): Boolean = {
    that match {
      case point: GPSPoint =>
        point.canEqual(this) && this.lng == point.lng && this.lat == point.lat
      case _ =>
        false
    }
  }


  def canEqual(other: Any) = other.isInstanceOf[GPSPoint]

  override def hashCode(): Int = {(lat.hashCode() + lng.hashCode()) / 2}

  override def toString: String = {
    "pointId: " + pointId + "; longitude: " + lng + "; latitude: " + lat
  }
}
