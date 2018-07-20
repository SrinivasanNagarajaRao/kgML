package net.kugou.algorithms.dbscan.exploratoryAnalysis


import net.kugou.algorithms.dbscan.{DbscanSettings, RawDataSet}
import net.kugou.algorithms.dbscan.spatial.{DistanceCalculation, Point, PointSortKey}
import net.kugou.algorithms.dbscan.spatial.rdd.{PartitioningSettings, PointsPartitionedByBoxesRDD}
import net.kugou.algorithms.dbscan.util.commandLine._
import net.kugou.algorithms.dbscan.util.debug.Clock
import net.kugou.algorithms.dbscan.util.io.IOHelper
import org.apache.spark.SparkContext
import org.apache.commons.math3.ml.distance.DistanceMeasure


/** A driver program which estimates distances to nearest neighbor of each point
 *
 */
object DistanceToNearestNeighborDriver extends DistanceCalculation {

  private [dbscan] class Args extends CommonArgs with NumberOfBucketsArg with NumberOfPointsInPartitionArg

  private [dbscan] class ArgsParser
    extends CommonArgsParser (new Args (), "DistancesToNearestNeighborDriver")
    with NumberOfBucketsArgParsing [Args]
    with NumberOfPointsInPartitionParsing [Args]

  def main (args: Array[String]) {
    val argsParser = new ArgsParser()

    if (argsParser.parse(args)) {
      val clock = new Clock()

      val sc = new SparkContext(argsParser.args.masterUrl,
        "Estimation of distance to the nearest neighbor",
        jars = Array(argsParser.args.jar))

      val data = IOHelper.readDataset(sc, argsParser.args.inputPath)
      val settings = new DbscanSettings().withDistanceMeasure(argsParser.args.distanceMeasure)
      val partitioningSettings = new PartitioningSettings(numberOfPointsInBox = argsParser.args.numberOfPoints)
      
      val histogram = createNearestNeighborHistogram(data, settings, partitioningSettings)
      
      val triples = ExploratoryAnalysisHelper.convertHistogramToTriples(histogram)

      IOHelper.saveTriples(sc.parallelize(triples), argsParser.args.outputPath)

      clock.logTimeSinceStart("Estimation of distance to the nearest neighbor")
    }
  }

  /**
   * This method allows for the histogram to be created and used within an application.
   */
  def createNearestNeighborHistogram(
    data: RawDataSet,
    settings: DbscanSettings = new DbscanSettings(),
    partitioningSettings: PartitioningSettings = new PartitioningSettings()) = {
    
    val partitionedData = PointsPartitionedByBoxesRDD(data, partitioningSettings)
    
    val pointIdsWithDistances = partitionedData.mapPartitions {
      it =>
        {
          calculateDistancesToNearestNeighbors(it, settings.distanceMeasure)
        }
    }

    ExploratoryAnalysisHelper.calculateHistogram(pointIdsWithDistances)
  }

  private[dbscan] def calculateDistancesToNearestNeighbors(
    it: Iterator[(PointSortKey, Point)],
    distanceMeasure: DistanceMeasure) = {

    val sortedPoints = it
      .map ( x => new PointWithDistanceToNearestNeighbor(x._2) )
      .toArray
      .sortBy( _.distanceFromOrigin )

    var previousPoints: List[PointWithDistanceToNearestNeighbor] = Nil

    for (currentPoint <- sortedPoints) {

      for (p <- previousPoints) {
        val d = calculateDistance(currentPoint, p)(distanceMeasure)

        if (p.distanceToNearestNeighbor > d) {
          p.distanceToNearestNeighbor = d
        }

        if (currentPoint.distanceToNearestNeighbor > d) {
          currentPoint.distanceToNearestNeighbor = d
        }
      }

      previousPoints = currentPoint :: previousPoints.filter {
        p => {
          val d = currentPoint.distanceFromOrigin - p.distanceFromOrigin
          p.distanceToNearestNeighbor >= d
        }
      }
    }

    sortedPoints.filter( _.distanceToNearestNeighbor < Double.MaxValue).map ( x => (x.pointId, x.distanceToNearestNeighbor)).iterator
  }
}
