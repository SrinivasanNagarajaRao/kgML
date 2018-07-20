package net.kugou.algorithms.dbscan.spatial

import net.kugou.algorithms.dbscan.{ClusterId, TempPointId}


/** A subclass of [[net.kugou.algorithms.dbscan.spatial.Point]] used during calculation of clusters within one partition
  *
  * @param p
  */
private [dbscan] class PartiallyMutablePoint (p: Point, val tempId: TempPointId) extends Point (p) {

  var transientClusterId: ClusterId = p.clusterId
  var visited: Boolean = false

  def toImmutablePoint: Point = new Point (this.coordinates, this.pointId, this.boxId, this.distanceFromOrigin,
    this.precomputedNumberOfNeighbors, this.transientClusterId)

}
