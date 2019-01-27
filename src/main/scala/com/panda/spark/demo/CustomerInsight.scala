package com.panda.spark.demo

import org.apache.spark.sql.types.{StructField, _}

object CustomerInsight {
  def main(args: Array[String]): Unit = {
    val spark=SparkUtils.getSparkSession
    import spark.implicits._
    val products=spark.read.option("delimiter","|").schema(productSchema).
      format("csv").load("inputs/products.dat")//.show(false)
    val customers=spark.read.option("delimiter","|").schema(customerSchema).
      format("csv").load("inputs/customers.dat")//.show(false)
    val sales=spark.read.option("delimiter","|").schema(salesSchema).
      format("csv").load("inputs/sales.dat")//.show(false)
    val refund=spark.read.option("delimiter","|").schema(refundSchema).
      format("csv").load("inputs/refund.dat")//.show(false)


  }





  def productSchema :StructType={
    StructType(
      Seq(
        StructField("productId",IntegerType,false),
        StructField("productName",StringType,false),
        StructField("productTYpe",StringType,false),
        StructField("productDescription",StringType,false),
        StructField("productPrice",IntegerType,false)
      )
    )
  }
  def customerSchema :StructType={
    StructType(
      Seq(
        StructField("customerId",IntegerType,false),
        StructField("firstName",StringType,false),
        StructField("lastNmae",StringType,false),
        StructField("phoneNumber",LongType,false)
      )
    )
  }
  def salesSchema  :StructType={
    StructType(
      Seq(
        StructField("transactionId",IntegerType,false),
        StructField("customerId",IntegerType,false),
        StructField("productId",IntegerType,false),
        StructField("timeStamp",StringType,false),
        StructField("totalAmount",IntegerType,false),
        StructField("totalQuantity",IntegerType,false)

      )
    )
  }
  def refundSchema  :StructType={
    StructType(
      Seq(
        StructField("refundId",IntegerType,false),
        StructField("originalTransactionId",IntegerType,false),
        StructField("customerId",IntegerType,false),
        StructField("productId",IntegerType,false),
        StructField("timestamp",StringType,false),
        StructField("refundAmount",IntegerType,false),
        StructField("refundQuantity",IntegerType,false)
      )
    )
  }
}
