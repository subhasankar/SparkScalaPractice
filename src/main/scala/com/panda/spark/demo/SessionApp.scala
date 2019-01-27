package com.panda.spark.demo
import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer

case class ClickStream(logTime: Timestamp, userId: String)
case class ClickStreamSession(userId: String, logTime: Timestamp, timeSpent:Integer,sessionId:String, logMonth:String,logDate:String)
object SessionApp {
  def main(args: Array[String]) {
    val sc = SparkUtils.getSparkSession
    import sc.implicits._

    val clickEncoder = Encoders.bean(classOf[ClickStream])
    //Can be read from hive with sql
    val clicks: Dataset[ClickStream] = sc.read.option("inferSchema", "true").option("header", "true").csv("ClickStream.csv").
      as[ClickStream](clickEncoder)
    val clicksMin=clicks.groupBy("userId").agg(collect_list("logTIme").as("allList"),min("logTime").as("minTime"));
    clicksMin.printSchema()


    def enrichSession(row: Row): Option[Array[ClickStreamSession]] = {
      val l = row.getList[Timestamp](1)
      val userId = row.getString(0)
      var c = 1
      var totalMin = row.getTimestamp(2)
      var min = row.getTimestamp(2)
      val a = l.toArray(Array.ofDim[Timestamp](l.size))
      var all = ListBuffer[ClickStreamSession]()
      a.foreach(v => {
        val inCount=(v.getTime - min.getTime) / (60 * 1000)
        val actCount=(v.getTime - totalMin.getTime) / (60 * 1000)
        if ( inCount> 30 ||  actCount> 120) {
          c += 1
          all += ClickStreamSession( userId,v,1, userId+"_"+c,v.toLocalDateTime.getYear+"-"+v.toLocalDateTime.getMonthValue,v.toLocalDateTime.getDayOfMonth.toString)
          min = v
          totalMin = v
        }
        else {
          min = v
          all += ClickStreamSession( userId,v,actCount.toInt, userId+"_"+c,v.toLocalDateTime.getYear+"-"+v.toLocalDateTime.getMonthValue,v.toLocalDateTime.getDayOfMonth.toString)
        }

      })
      Some(all.toArray)
    }

    val finalStream=clicksMin.flatMap(enrichSession).flatMap(x=>x).cache();
    finalStream.show(false)
    Thread.sleep(40000)
    //    finalStream.printSchema()
    //    finalStream.write.partitionBy("logMonth","logDate").parquet("path")
    //    //Can be read from hive with sql
    //    finalStream.createOrReplaceGlobalTempView("finalStream")
    //    sc.sql("select userId, count(distinct(sessionId)) as dcount from global_temp.finalStream group by userId").show(false);
    //    sc.sql("select userId,logMonth,logDate, sum(timeSpent) as totalTimeSpent from global_temp.finalStream group by userId,logMonth,logDate").show(false);
    //    sc.sql("select userId,logMonth, sum(timeSpent) as totalTimeSpent from global_temp.finalStream group by userId,logMonth").show(false);
    //


  }

}