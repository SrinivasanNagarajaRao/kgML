package net.kugou.algorithms.dbscan

import net.kugou.algorithms.dbscan.spatial.{DistanceAnalyzer, Point}
import org.apache.commons.math3.ml.distance._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.io.File

/** Represents results calculated by DBSCAN algorithm.
  *
  * This object is returned from [[net.kugou.algorithms.dbscan.Dbscan.run]] method.
  * You cannot instantiate it directly
  */
class DbscanModel private[dbscan] (val allPoints: RDD[Point],
val settings: DbscanSettings)
  extends Serializable {



  /** Predicts which cluster a point would belong to
    *
    * @param newPoint A [[net.kugou.algorithms.dbscan.PointCoordinates]] for which you want to make a prediction
    * @return If the point can be assigned to a cluster, then this cluster's ID is returned.
    *         If the point appears to be a noise point, then
    *         [[net.kugou.algorithms.dbscan.DbscanModel.NoisePoint]] is returned.
    *         If the point is surrounded by so many noise points that they can constitute a new
    *         cluster, then [[net.kugou.algorithms.dbscan.DbscanModel.NewCluster]] is returned
    */
  def predict (newPoint: Point): ClusterId = {
    val distanceAnalyzer = new DistanceAnalyzer(settings)
    val neighborCountsByCluster = distanceAnalyzer.findNeighborsOfNewPoint(allPoints, newPoint.coordinates)
      .map ( x => (x.clusterId, x) )
      .countByKey()

    val neighborCountsWithoutNoise = neighborCountsByCluster.filter(_._1 != DbscanModel.NoisePoint)
    val possibleClusters = neighborCountsWithoutNoise.filter(_._2 >= settings.numberOfPoints-1)
    val noisePointsCount = if (neighborCountsByCluster.contains(DbscanModel.NoisePoint)) {
      neighborCountsByCluster (DbscanModel.NoisePoint)
    }
    else {
      0L
    }

    if (possibleClusters.nonEmpty) {

      // If a point is surrounded by >= numPts points which belong to one cluster, then the point should be assigned to that cluster
      // If there are more than one clusters, then the cluster will be chosen arbitrarily

      possibleClusters.keySet.head
    }
    else if (neighborCountsWithoutNoise.nonEmpty && !settings.treatBorderPointsAsNoise) {

      // If there is not enough surrounding points, then the new point is a border point of a cluster
      // In this case, the prediction depends on treatBorderPointsAsNoise flag.
      // If it allows assigning border points to clusters, then the new point will be assigned to the cluster
      // If there are many clusters, then one of them will be chosen arbitrarily

      neighborCountsWithoutNoise.keySet.head
    }
    else if (noisePointsCount >= settings.numberOfPoints-1) {

      // The point is surrounded by sufficiently many noise points so that together they will constitute a new cluster

      DbscanModel.NewCluster
    }
    else {

      // If none of the above conditions are met, then the new point is noise

      DbscanModel.NoisePoint
    }
  }


  /**
    * save the model to specified path
    *
    * @param path path for the model
    */
  def save(path: String): Unit ={
    val data: RDD[String] = this.allPoints.map{point =>
      val pointId: PointId = point.pointId
      val clusterId: ClusterId = point.clusterId
      val boxId: BoxId = point.boxId
      val jwd: String = point.coordinates.mkString(",")
      val originDist: Double = point.distanceFromOrigin
      val neighborCnts: ClusterId = point.precomputedNumberOfNeighbors

      Array(pointId, clusterId, boxId, jwd, originDist, neighborCnts).mkString(";")
    }

    val epsilon: Double = this.settings.epsilon
    val pointCnts: BoxId = this.settings.numberOfPoints
    val isBorderNoise: Boolean = this.settings.treatBorderPointsAsNoise
    val distMeature: String = this.settings.distanceMeasure match {
      case _: EuclideanDistance => "EuclideanDistance"
      case _: CanberraDistance => "CanberraDistance"
      case _: ChebyshevDistance => "ChebyshevDistance"
      case _: EarthMoversDistance => "EarthMoversDistance"
      case _: ManhattanDistance => "ManhattanDistance"
      case _ => ""
    }

    val settings: String = epsilon + ";" + pointCnts + ";" + isBorderNoise + ";" + distMeature

    data.sparkContext.parallelize(Seq(settings)).saveAsTextFile(path + File.separator + "settings")
    data.saveAsTextFile(path + File.separator + "points")
  }


  /**
    * load DbscanModel from specified path
    *
    * @param sc SparkContext
    * @param path path for the model
    * @return DbscanModel
    */
  def load(sc: SparkContext, path: String): DbscanModel = {
    val tokens: Array[String] = sc.textFile(path + File.separator + "settings").collect()(0).split(";")
    val epsilon: Double = tokens(0).toDouble
    val pointCnts: BoxId = tokens(1).toInt
    val isBorderNoise: Boolean = tokens(2).toBoolean
    val distMeature: String = tokens(3)

    val tmpSetting: DbscanSettings = new DbscanSettings()
      .withEpsilon(epsilon)
      .withNumberOfPoints(pointCnts)
      .withTreatBorderPointsAsNoise(isBorderNoise)

    distMeature match {
      case "EuclideanDistance" => tmpSetting.withDistanceMeasure(new EuclideanDistance())
      case "CanberraDistance" => tmpSetting.withDistanceMeasure(new CanberraDistance())
      case "ChebyshevDistance" => tmpSetting.withDistanceMeasure(new ChebyshevDistance())
      case "EarthMoversDistance" => tmpSetting.withDistanceMeasure(new EarthMoversDistance())
      case "ManhattanDistance" => tmpSetting.withDistanceMeasure(new ManhattanDistance())
    }

    val points: RDD[Point] = sc.textFile(path + File.separator + "points").flatMap{line =>
      val terms: Array[String] = line.split(";")

      if (terms.length > 5) {
        val pointId: ClusterId = terms(0).toLong
        val clusterId: ClusterId = terms(1).toLong
        val boxId: BoxId = terms(2).toInt
        val jwd = terms(3).split(",").map(_.toDouble)
        val originDist: Double = terms(4).toDouble
        val neighborCnts: ClusterId = terms(5).toLong

        val coordinates: PointCoordinates = new PointCoordinates(jwd)
        Some(new Point(coordinates, pointId, boxId, originDist, neighborCnts, clusterId))
      } else None
    }

    new DbscanModel(points, settings)
  }


  /** Returns only noise points
    *
    * @return
    */
  def noisePoints: RDD[Point] = { allPoints.filter(_.clusterId == DbscanModel.NoisePoint) }

  /** Returns points which were assigned to clusters
    *
    * @return
    */
  def clusteredPoints: RDD[Point] = { allPoints.filter( _.clusterId != DbscanModel.NoisePoint) }
}

/** Contains constants which designate cluster ID
  *
  */
object DbscanModel {

  /** Designates noise points
    *
    */
  val NoisePoint: ClusterId = 0

  /** Indicates that a new cluster would appear in a [[net.kugou.algorithms.dbscan.DbscanModel]] if
    *  a new point was added to it
    */
  val NewCluster: ClusterId = -1

  /** Initial value for cluster ID of each point.
    *
    */
  private [dbscan] val UndefinedCluster: ClusterId = -2


  def load(sc: SparkContext, path: String): DbscanModel = this.load(sc, path)
}
