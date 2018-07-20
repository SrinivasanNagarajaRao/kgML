package net.kugou.algorithms.dbscan.util.io

import net.kugou.algorithms.dbscan.spatial.Point
import net.kugou.algorithms.dbscan.{DbscanModel, RawDataSet}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/** Contains functions for reading and writing data
  *
  */
object IOHelper {

  /** Reads a dataset from a CSV file. That file should contain double values separated by commas
    *
    * @param sc A SparkContext into which the data should be loaded
    * @param path A path to the CSV file
    * @return A [[net.kugou.algorithms.dbscan.RawDataSet]] populated with points
    */
  def readDataset (sc: SparkContext, path: String): RawDataSet = {
    val rawData = sc.textFile (path)

    rawData.map (
      line => {
        new Point (line.split(separator).map( _.toDouble ))
      }
    )
  }


  /** Transform a dataframe to RawDataSet. Now, this function just support two features: longitude and latitude,
    * and you can assign them by the parameters
    *
    * @param data The dataframe to be transformed
    * @param longitudeCol longitude column name
    * @param latitudeCol  latitude column name
    * @return A [[net.kugou.algorithms.dbscan.RawDataSet]] populated with points
    */
  def dataTransform(data: DataFrame, longitudeCol: String, latitudeCol: String): RawDataSet = {
    data.select(longitudeCol, latitudeCol).rdd.map{row =>
      val longitude: Double = row.getDouble(0)
      val latitude: Double = row.getDouble(1)
      new Point(Array(longitude, latitude))
    }
  }


  /** Saves clustering result into a CSV file. The resulting file will contain the same data as the input file,
    * with a cluster ID appended to each record. The order of records is not guaranteed to be the same as in the
    * input file
    *
    * @param model      A [[net.kugou.algorithms.dbscan.DbscanModel]] obtained from Dbscan.train method
    * @param outputPath Path to a folder where results should be saved. The folder will contain multiple
    *                   partXXXX files
    */
  def saveClusteringResult (model: DbscanModel, outputPath: String) {

    model.allPoints.map ( pt => {

      pt.coordinates.mkString(separator) + separator + pt.clusterId
    } ).saveAsTextFile(outputPath)
  }

  private [dbscan] def saveTriples (data: RDD[(Double, Double, Long)], outputPath: String) {
    data.map ( x => x._1 + separator + x._2 + separator + x._3 ).saveAsTextFile(outputPath)
  }

  private def separator = ","

}
