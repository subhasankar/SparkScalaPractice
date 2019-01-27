package com.panda.spark.demo

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession

object SparkUtils {
def getSparkSession= {
  val spark = SparkSession.builder().
    appName("spark-runner")
    .config("spark.master", "local")
    .config("spark.submit.deployMode", "client")
    .config("spark.sql.shuffle.partitions", "2")
    //.enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.addSparkListener(new SparkListener() {
    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
      synchronized {
        println("******************")
        println(taskEnd.taskMetrics.outputMetrics.recordsWritten)
      }
    }
  })
  spark
}
}
