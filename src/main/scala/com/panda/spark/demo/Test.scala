package com.panda.spark.demo

import org.apache.log4j.Logger
import org.slf4j.LoggerFactory

object Test {
val log=LoggerFactory.getLogger(getClass)
  def main(args: Array[String]): Unit = {
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
