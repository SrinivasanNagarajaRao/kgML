package net.kugou.algorithms.dbscan.exploratoryAnalysis

import net.kugou.algorithms.dbscan.spatial.Point


private [dbscan] class PointWithDistanceToNearestNeighbor (pt: Point, d: Double = Double.MaxValue) extends  Point (pt) {
  var distanceToNearestNeighbor = d
}
