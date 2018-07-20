package net.kugou.algorithms.dbscan.util.commandLine

import net.kugou.algorithms.dbscan.exploratoryAnalysis.ExploratoryAnalysisHelper


private [dbscan] trait NumberOfBucketsArg {
  var numberOfBuckets: Int = ExploratoryAnalysisHelper.DefaultNumberOfBucketsInHistogram
}
