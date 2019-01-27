package com.panda.spark.demo

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object UrlTrafic {
  def main(args: Array[String]): Unit = {
    val spark=SparkUtils.getSparkSession
    import spark.implicits._

    val trafics=spark.read.option("delimiter","\t").schema(urlTrafic).format("csv").load("inputs/url_trafic.dat")
    trafics.groupBy("url").agg(count("userId")).show(false)
  }
  def urlTrafic :StructType={
    StructType(
      Seq(
        StructField("userId",IntegerType,false),
        StructField("url",StringType,false),
        StructField("logDate",TimestampType,false)
      )
    )
  }
}
