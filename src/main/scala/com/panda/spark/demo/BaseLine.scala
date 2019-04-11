package com.panda.spark.demo

import java.sql.Date

import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object BaseLine {

  val numOfWeeks=3
  def main(args: Array[String]): Unit = {
    //getting the spark session from the util class
    val spark=SparkUtils.getSparkSession
    //below code to read data either from csv or hive sql
    val salesData=readData(spark)
    //code for all the processing
    val baseData=calcBasePriceAndPromo(salesData)
    //Finally storing to hive or hdfs file
    storeBaseData(baseData)
    System.in.read
  }




  def readData(spark: SparkSession) = {
    val salesDataSchema = Encoders.product[SalesData]
    spark.read.option("delimiter",",").option("header", "true").schema(salesDataSchema.schema).csv("input/Sample.csv").as(salesDataSchema)
   // spark.sql("select calendar_dt,upc,calendar_week_nbr,calendar_year_short_desc,store,gm_brand_cn,gm_brand_en,gm_category_cn,gm_category_en,gm_subcategory_cn,gm_subcategory_en,gm_channel from sales.tableName").as(salesDataSchema)
  }

  def calcBasePriceAndPromo(salesData: Dataset[SalesData]) = {
    //window spec for base price
    val windowSpec = Window.partitionBy("upc", "store", "gm_brand_cn", "gm_brand_en", "gm_category_cn", "gm_category_en", "gm_subcategory_cn", "gm_subcategory_en", "gm_channel").orderBy(col("calendar_year_short_desc").desc,col("calendar_week_nbr").desc)


    //remove the next 2 filters and uncomment the 3rd line
    val filteredData=salesData.where(salesData.col("upc").equalTo("00506110_RTM")
      .and(salesData.col("store").equalTo("01-0085")))
    //val filteredData= salesData

    //creating aggregated columns
    val aggData=filteredData.groupBy("upc","calendar_week_nbr","calendar_year_short_desc","store","gm_brand_cn",
      "gm_brand_en","gm_category_cn","gm_category_en","gm_subcategory_cn","gm_subcategory_en","gm_channel").
        agg(sum("total_sales_amount").as("total_sales_amount"),
        sum("total_sales_volume_units").as("total_sales_volume_units"),
        round(sum("total_sales_amount")/sum("total_sales_volume_units")).as("avg_price"))

    aggData
      .withColumn("basePriceX", array((1 to 3).map(e=>coalesce(lead("avg_price",e).over(windowSpec),lead("avg_price",e-3).over(windowSpec))): _*))
      .withColumn("base_price", greatest((1 to 3).map(e=>coalesce(lead("avg_price",e).over(windowSpec),lead("avg_price",e-3).over(windowSpec))): _*))
      .withColumn("discount",col("base_price").minus(col("avg_price")))
      .withColumn("percentage",round((col("discount").divide(col("base_price"))).multiply(100)))
      .withColumn("promo_flag",when(col("percentage").geq(5),"1").otherwise("0"))


  }

  def storeBaseData(baseData: DataFrame)= {
    baseData.show(false)
    //baseData.createOrReplaceTempView("base_data");
    //spark.sql("create table final_sales_data as select * from base_data")
    //baseData.coalesce(1).write.option("header","true").csv("")

  }

  //Udf to find the max value from the list -- this is not used as greatest function is same
//val maxUdf= udf {
//  s: Seq[Double] =>
//    s.max
//}

}


case class SalesData(calendar_dt:Date,calendar_week_nbr:Int,calendar_year_short_desc:Int, upc:String,gm_brand_cn:String , gm_brand_en :String, gm_category_cn:String , gm_category_en:String , gm_subcategory_cn:String , gm_subcategory_en :String, store :String, gm_channel:String , total_sales_volume_units :Double, total_sales_amount:Double )

