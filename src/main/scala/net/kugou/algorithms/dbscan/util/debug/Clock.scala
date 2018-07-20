package net.kugou.algorithms.dbscan.util.debug

private [dbscan] class Clock {
  val startTime = System.currentTimeMillis()

  def logTimeSinceStart (): Unit = {
    logTimeSinceStart("Test")
  }

  def logTimeSinceStart (message: String) = {
    val currentTime = System.currentTimeMillis()
    val timeSinceStart = (currentTime - startTime) / 1000.0

    println(s"$message took $timeSinceStart seconds")
  }
}
