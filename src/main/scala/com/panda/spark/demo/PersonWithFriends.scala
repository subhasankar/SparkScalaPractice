package com.panda.spark.demo

import java.sql.Timestamp
import java.time.{LocalDate, ZoneId}
import java.util.Date

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

case class PersonInput(id:Int,name:String,age:Int,friends:String)
case class PersonFriend(id:Int,name:String,age:Int,friendName:String,fAge:Int,fdate:Long)
case class Friend(name:String,age:Int,date:Timestamp)
case class Person(id:Int,name:String,age:Int,friends:Array[Friend])

object PersonWithFriends {
  def main(args: Array[String]): Unit = {
    val spark=SparkUtils.getSparkSession
    import spark.implicits._
    val personSchema=Encoders.product[PersonInput].schema
    val persons=spark.read.schema(personSchema).json("inputs/person.json").show(false)
  // persons.withColumn("friendsAb", explode($"friends")).show(false)//withColumn("ageDiff",abs(col("age").minus(col("friends.age")))).orderBy(col("friends.date").asc,col("ageDiff").desc).show(false)
   // persons.printSchema()
   //spark.read.csv("inputs/Person.dat").show()
   // println(spark.sparkContext.parallelize(List("abc","csadaw")).map(a=>(a,1)).groupByKey().count())
//person1.show(false)
    //persons.map(x=>(x,2)).collect()


        val personsCsv=spark.read.format("csv").option("delimiter","|").schema(personSchema).load("inputs/Person.dat").show(false)
     // persons.map(a=>(a.getInt(0),1)).groupByKey(a=>a._1).count().show(false)
    System.in.read

    spark.stop
    //persons.groupByKey(a=>)
//    val mappedPerson=persons.flatMap(a=>{
//      var l:ArrayBuffer[PersonFriend]=ArrayBuffer()
//      val allF=a.getString(3).substring(2,a.getString(3).length-2).split("\\),\\(")
//      allF.foreach(d=>{
//        val f=d.split(",")
//        val c=f(2).split("\\/")
//        l+=PersonFriend(a.getInt(0),a.getString(1),a.getInt(2),f(0),f(1).toInt,LocalDate.of(c(2).toInt,c(1).toInt,c(0).toInt).atStartOfDay(ZoneId.systemDefault).toEpochSecond())
//      })
//      Some(l.toArray)
//    })//.flatMap(x=>x)
//    mappedPerson.orderBy(asc("fdate"),desc("fAge")).show(false)
    //employees.show(false)
    //employees.printSchema()
  }

}
