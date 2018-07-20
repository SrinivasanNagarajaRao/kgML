package net.kugou.algorithms.dbscan.util.commandLine

import net.kugou.algorithms.dbscan.DbscanSettings


private [dbscan] trait EpsArg {
  var eps: Double = DbscanSettings.getDefaultEpsilon
}
