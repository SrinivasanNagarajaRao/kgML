package net.kugou.algorithms.dbscan.spatial.rdd

import net.kugou.algorithms.dbscan.{BoxId, PairOfAdjacentBoxIds}
import net.kugou.algorithms.dbscan.spatial.{Box, BoxCalculator}
import org.apache.spark.Partitioner

/** Partitions an [[net.kugou.algorithms.dbscan.spatial.rdd.PointsInAdjacentBoxesRDD]] so that each partition
  * contains points which reside in two adjacent boxes
 *
 * @param adjacentBoxIdPairs
 */
private [dbscan] class AdjacentBoxesPartitioner (private val adjacentBoxIdPairs: Array[PairOfAdjacentBoxIds]) extends Partitioner {

  def this (boxesWithAdjacentBoxes: Iterable[Box]) = this (BoxCalculator.generateDistinctPairsOfAdjacentBoxIds(boxesWithAdjacentBoxes).toArray)

  override def numPartitions: Int = adjacentBoxIdPairs.length

  override def getPartition(key: Any): Int = {
    key match {
      case (b1: BoxId, b2: BoxId) => adjacentBoxIdPairs.indexOf((b1, b2))
      case _ => 0 // Throw an exception?
    }
  }
}
