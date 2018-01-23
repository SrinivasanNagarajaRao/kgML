package net.kugou

import net.kugou.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  *
  * Created by yhao on 2018/01/17 17:52.
  */
object SparkSQLDemo extends Serializable {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkUtils().createSparkEnv()
    args.foreach{param =>
      println("执行SQL: " + param)
      spark.sql(param)
    }

    spark.stop()
  }
}
