package spark.example

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties
import scala.io.Source

object OrderProcess extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    logger.info("Starting spark ..")
    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()
    logger.info("Stopping Spark")
    spark.stop()
  }

  def getSparkAppConf : SparkConf = {

    val sparkAppConf = new SparkConf
    val props = new Properties()

    //read from the sparkConf config file and set config in sparkConf
    props.load(Source.fromFile("spark.conf").bufferedReader())

    props.forEach((k, v) => sparkAppConf.set(k.toString, v.toString))
    //This is a fix for Scala 2.11
    //import scala.collection.JavaConverters._
    //props.asScala.foreach(kv => sparkAppConf.set(kv._1, kv._2))
    sparkAppConf
  }
}
