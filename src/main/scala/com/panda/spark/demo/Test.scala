package com.panda.spark.demo

import java.sql.{Date, Timestamp}

import com.google.gson.Gson
import org.apache.log4j.Logger
import org.slf4j.LoggerFactory

object Test {
val log=LoggerFactory.getLogger(getClass)
  case class Friend(name:String,age:Int,date:Timestamp)
  case class Person(id:Int,name:String,age:Int,friends:Array[Friend])

  def main(args: Array[String]): Unit = {
    val p=Person(1,"abc",25,Array(Friend("suv",25,new Timestamp(0)),Friend("awa",26,new Timestamp(0))))
    val gson = new Gson()
    val jsonString = gson.toJson(p)
    println(jsonString)
    log.debug("this is nt working")
    val a=List(("d",2),("c",3))
    a.foreach(k=>{
      if(k._2>2){
        println(k._2+5)
        k._2+5
      }
    })
    println("this is"+a.tail)
    println(a.last)
  }
}
