
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
/**
  *
  * Created by yhao on 2017/10/25 10:48.
  */
object Test {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    spark.stop()

  }
}
