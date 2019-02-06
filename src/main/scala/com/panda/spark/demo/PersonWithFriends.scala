package com.panda.spark.demo

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

case class PersonInput(id:Int,name:String,age:Int,friends:String)
case class Person(name:String,age:Int,dateM:String)
case class PersonFriend(id:Int,name:String,age:Int,person:Person)

object PersonWithFriends {
  def main(args: Array[String]): Unit = {
    val spark=SparkUtils.getSparkSession
    import spark.implicits._
    val personSchema=Encoders.product[PersonInput].schema
    val persons=spark.read.format("csv").option("delimiter","|").schema(personSchema).load("inputs/Person.dat")
    persons.flatMap(a=>{
      var l:ArrayBuffer[PersonFriend]=ArrayBuffer()
      val allF=a.getString(3).substring(2,a.getString(3).length-2).split("\\),\\(")
      allF.foreach(d=>{
        val f=d.split(",")
        l+=PersonFriend(a.getInt(0),a.getString(1),a.getInt(2),Person(f(0),f(1).toInt,f(2)))
      })
      Some(l.toArray)
    }).flatMap(x=>x).show(false)
    //employees.show(false)
    //employees.printSchema()
  }

}
