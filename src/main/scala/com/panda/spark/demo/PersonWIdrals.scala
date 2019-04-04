package com.panda.spark.demo

import com.panda.spark.demo.App.userSchema
import com.panda.spark.demo.TripCounts.format
import com.panda.spark.demo.entities.Users
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object PersonWIdrals {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession
    import spark.implicits._
    val users = spark.read.format("csv").option("delimiter", ",").option("header", "true").
      option("inferSchema", "true").load("inputs/emp.dat")
    users.printSchema()
    val ds=users.groupBy("empId","month").agg(sum("amount").as("total"))
    val w=Window.partitionBy("empId").orderBy("month")
    ds.withColumn("lastWid",lag("total",1,0) over(w)).show(false)

  }
}
