package net.kugou.algorithms.dbscan.spatial.rdd


import net.kugou.algorithms.dbscan.spatial.{Box, BoxCalculator, Point, PointSortKey}
import net.kugou.algorithms.dbscan.util.PointIndexer
import net.kugou.algorithms.dbscan.{DbscanSettings, PointCoordinates, PointId, RawDataSet}
import org.apache.spark.rdd.{RDD, ShuffledRDD}

/** Density-based partitioned RDD where each point is accompanied by its sort key
  *
  * @param prev
  * @param boxes
  * @param boundingBox
  */
private [dbscan] class PointsPartitionedByBoxesRDD  (prev: RDD[(PointSortKey, Point)], val boxes: Iterable[Box], val boundingBox: Box)
  extends ShuffledRDD [PointSortKey, Point, Point] (prev, new BoxPartitioner(boxes))

object PointsPartitionedByBoxesRDD {

  def apply (rawData: RawDataSet,
    partitioningSettings: PartitioningSettings = new PartitioningSettings (),
    dbscanSettings: DbscanSettings = new DbscanSettings ())
    : PointsPartitionedByBoxesRDD = {

    val sc = rawData.sparkContext
    val boxCalculator = new BoxCalculator (rawData)
    val (boxes, boundingBox) = boxCalculator.generateDensityBasedBoxes (partitioningSettings, dbscanSettings)
    val broadcastBoxes = sc.broadcast(boxes)
    val broadcastNumberOfDimensions = sc.broadcast(boxCalculator.numberOfDimensions)

    val pointsInBoxes = PointIndexer.addMetadataToPoints(
      rawData,
      broadcastBoxes,
      broadcastNumberOfDimensions,
      dbscanSettings.distanceMeasure)

    PointsPartitionedByBoxesRDD (pointsInBoxes, boxes, boundingBox)
  }

  def apply (pointsInBoxes: RDD[(PointSortKey, Point)], boxes: Iterable[Box], boundingBox: Box): PointsPartitionedByBoxesRDD = {
    new PointsPartitionedByBoxesRDD(pointsInBoxes, boxes, boundingBox)
  }


  private [dbscan] def extractPointIdsAndCoordinates (data: RDD[(PointSortKey, Point)]): RDD[(PointId, PointCoordinates)] = {
    data.map ( x => (x._2.pointId, x._2.coordinates) )
  }

}


