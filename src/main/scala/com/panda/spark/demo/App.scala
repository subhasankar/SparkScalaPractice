package com.panda.spark.demo

import com.panda.spark.demo.entities.{Transactions, Users}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

object App {
  val log=LoggerFactory.getLogger(getClass);
  def main(args: Array[String]): Unit = {
    log.info("Hello lets start spark coding in scala")
    val spark=SparkUtils.getSparkSession
    import spark.implicits._

    val users=spark.read.format("csv").option("delimiter"," ").schema(userSchema).load("inputs/users.dat").as[Users].select("id","location")

    val transactions=spark.read.format("csv").schema(transactionSchema).load("inputs/transaction.dat").as[Transactions].select("userId","productId")
    //transactions.show(false)


    transactions.joinWith(users,users("id")===transactions("userId")).select("_1.productId","_2.location")
       .groupBy("productId").agg(countDistinct("location")).show(false)
    System.in.read
    spark.stop
  }




  def userSchema :StructType={
    StructType(
      Seq(
        StructField("id",IntegerType,false),
        StructField("email",StringType,false),
        StructField("lang",StringType,false),
        StructField("location",StringType,false)
      )
    )
  }

  def transactionSchema :StructType={
    StructType(
      Seq(
        StructField("transactionId",IntegerType,false),
        StructField("productId",IntegerType,false),
        StructField("userId",IntegerType,false),
        StructField("purchaseAmount",IntegerType,false),
        StructField("itemDescription",StringType,false)
      )
    )
  }
}
