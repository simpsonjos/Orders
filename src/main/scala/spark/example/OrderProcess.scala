package spark.example

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, udf, to_date}
import org.apache.spark.sql.types.{DateType, DoubleType, FloatType, IntegerType, StringType, StructField, StructType}

import java.util.Properties
import scala.io.Source

object OrderProcess extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    logger.info("Starting spark ..")
    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()


    val orderHeaderSchema = StructType(List(
      StructField("orderNo", IntegerType),
      StructField("orderDate", StringType),
      StructField("orderStatus", StringType),
      StructField("orderTotal", DoubleType),
      StructField("shippingAddr", StringType),
      StructField("billingAddr", StringType),
      StructField("paymentMethod", StringType)
    ))

    //read json file
    val orderHeaderDF = spark.read
      .format("json")
      .option("path", "dataset/OrderHeader.json")
      .option("mode", "FAILFAST")
      //.option("multiline", "true")
      //.option("dateFormat", "M/d/y")
      .schema(orderHeaderSchema)
      .load()

    orderHeaderDF.show()
    /*
    val orderHeaderTransformed = orderHeaderDF.withColumn("orderStatusDesc",
      expr(
        "case when orderStatus ='R' then 'Received'" +
        " when orderStatus = 'S' then 'Shipped' " +
        " when orderStatus = 'D' then 'Delivered' " +
        " else 'Unknown' end"))
  */

    val parseOrderStatusUDF = udf(parseOrderStatus(_:String):String)

    val orderHeaderTransformed = orderHeaderDF
      .withColumn("orderStatus", parseOrderStatusUDF(col("orderStatus")) )
      .withColumn("orderDate", to_date(col("orderDate"), "M/d/y"))

    orderHeaderTransformed.show()

    logger.info("Stopping Spark")
    scala.io.StdIn.readLine()
    spark.stop()
  }

  def parseOrderStatus(s: String): String = {
    if (s.equals("R")) "Received"
    else if (s.equals(("S"))) "Shipped"
    else if (s.equals("D")) "Delivered"
    else "Unknown"
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
