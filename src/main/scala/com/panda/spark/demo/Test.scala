package com.panda.spark.demo

import org.apache.log4j.Logger
import org.slf4j.LoggerFactory

object Test {
val log=LoggerFactory.getLogger(getClass)
  def main(args: Array[String]): Unit = {
    log.debug("this is nt working")

  }
}
