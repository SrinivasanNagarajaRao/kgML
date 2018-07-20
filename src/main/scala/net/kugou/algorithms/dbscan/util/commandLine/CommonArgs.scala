package net.kugou.algorithms.dbscan.util.commandLine

import net.kugou.algorithms.dbscan.DbscanSettings
import org.apache.commons.math3.ml.distance.DistanceMeasure

private [dbscan] class CommonArgs (
  var masterUrl: String = null,
  var jar: String = null,
  var inputPath: String = null,
  var outputPath: String = null,
  var distanceMeasure: DistanceMeasure = DbscanSettings.getDefaultDistanceMeasure,
  var debugOutputPath: Option[String] = None)
