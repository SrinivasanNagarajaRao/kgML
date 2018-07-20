package net.kugou.algorithms.dbscan.spatial

private [dbscan] class PointSortKey (pt: Point) extends  Ordered[PointSortKey] with Serializable {
  val boxId = pt.boxId
  val pointId = pt.pointId

  override def compare(that: PointSortKey): Int = {

    if (this.boxId > that.boxId) {
      1
    }
    else if (this.boxId < that.boxId) {
      -1
    }
    else if (this.pointId > that.pointId) {
      1
    }
    else if (this.pointId < that.pointId) {
      -1
    }
    else {
      0
    }
  }

  override def equals (that: Any): Boolean = {

    that match {
      case key: PointSortKey =>
        key.canEqual(this) &&
          this.pointId == key.pointId &&
          this.boxId == key.boxId
      case _ =>
        false
    }
  }

  override def hashCode (): Int = {
    41 * (41 * pointId.toInt) + boxId
  }

  def canEqual(other: Any) = other.isInstanceOf[PointSortKey]

  override def toString: String = {
    s"PointSortKey with box: $boxId , ptId: $pointId"
  }
}
