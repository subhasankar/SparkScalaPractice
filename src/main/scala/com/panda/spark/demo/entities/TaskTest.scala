package com.panda.spark.demo.entities

import com.panda.spark.demo.SparkUtils

object TaskTest {
  def main(args: Array[String]): Unit = {
    val spark=SparkUtils.getSparkSession
    import spark.implicits._
    spark.sparkContext.getConf.set("spark.sql.shuffle.partitions", "2")
    val input=spark.sparkContext.parallelize(1 to 100,4)
    val dsInput=input.toDS()
    val a=dsInput.map(m=>(m,1)).groupByKey(r=>r._1).mapValues(a=>{println(a)
        a
    }).count()
    a.write.format("csv").save("/user/bdauser/subha/test/1")
    System.in.read
    spark.stop
  }
}
