package net.kugou.algorithms.dbscan.spatial

import net.kugou.algorithms.dbscan.BoxId


private [dbscan] class BoxIdGenerator (val initialId: BoxId) {
  var nextId = initialId

  def getNextId (): BoxId = {
    nextId += 1
    nextId
  }
}
