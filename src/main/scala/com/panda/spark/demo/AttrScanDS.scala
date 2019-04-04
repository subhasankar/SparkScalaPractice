package com.panda.spark.demo

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Row}
object AttrScanDS {
 def main(args: Array[String]): Unit = {
   val spark = SparkUtils.getSparkSession
   import spark.implicits._
   val all=spark.read.format("csv").option("delimiter"," ").load("inputs/users.dat")
   val attributes = all.filter(a=>{a.getString(0).startsWith("A")})
   val others =all.filter(a=>{!a.getString(0).startsWith("A")})


 }
}
