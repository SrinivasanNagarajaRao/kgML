package net.kugou.algorithms.dbscan.spatial

private [dbscan] class BoxTreeItemWithNumberOfPoints (b: Box) extends BoxTreeItemBase [BoxTreeItemWithNumberOfPoints] (b) {

  var numberOfPoints: Long = 0

  override def clone (): BoxTreeItemWithNumberOfPoints  = {

    val result = new BoxTreeItemWithNumberOfPoints (this.box)
    result.children = this.children.map { x => x.clone () }

    result
  }

}
