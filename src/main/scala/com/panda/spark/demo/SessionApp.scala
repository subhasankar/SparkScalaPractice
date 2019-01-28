package com.panda.spark.demo
import java.sql.Timestamp

import com.panda.spark.demo.entities.{ClickStream, ClickStreamSession}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer


object SessionApp {

  def main(args: Array[String]) {
    val sc = SparkUtils.getSparkSession
    import sc.implicits._

    val clickEncoder = Encoders.bean(classOf[ClickStream])
    val clickStreamsEncoder = Encoders.bean(classOf[ClickStreamSession])

    //Can be read from hive with sql
    val clicks = sc.read.option("inferSchema", "true").option("header", "true").csv("inputs/ClickStream.csv")

    def enrichSession(a:String,b:Iterator[Row]): Option[Array[ClickStreamSession]] = {
      val values=b.toList.map(d=>{(d.getString(1),d.getTimestamp(0))}).sortWith((a,b)=>((a._2.getTime<b._2.getTime)))
      var min,totalMin = values.head._2
      var c=1
      val all = ListBuffer[ClickStreamSession]()
      values.foreach(v => {
        val inCount=(v._2.getTime - min.getTime) / (60 * 1000)
        val actCount=(v._2.getTime - totalMin.getTime) / (60 * 1000)
        if ( inCount > 30 ||  actCount> 120) {
          c += 1
          all += ClickStreamSession( v._1,v._2,1, v._1+"_"+c,v._2.toLocalDateTime.getYear+"-"+v._2.toLocalDateTime.getMonthValue,v._2.toLocalDateTime.getDayOfMonth.toString)
          min,totalMin = v._2
        }
        else {
          min = v._2
          all += ClickStreamSession( v._1,v._2,actCount.toInt, v._1+"_"+c,v._2.toLocalDateTime.getYear+"-"+v._2.toLocalDateTime.getMonthValue,v._2.toLocalDateTime.getDayOfMonth.toString)
        }
      })
      Some(all.toArray)
    }

    val finalStream=clicks.groupByKey(r=>r.getString(1)).flatMapGroups(enrichSession).flatMap(z=>z)
    finalStream.cache()

    //    finalStream.printSchema()
    //    finalStream.write.partitionBy("logMonth","logDate").parquet("path")
    //    //Can be read from hive with sql
        finalStream.createOrReplaceGlobalTempView("finalStream")
        sc.sql("select userId, count(distinct(sessionId)) as dcount from global_temp.finalStream group by userId").show(false);
        sc.sql("select userId,logMonth,logDate, sum(timeSpent) as totalTimeSpent from global_temp.finalStream group by userId,logMonth,logDate").show(false);
        sc.sql("select userId,logMonth, sum(timeSpent) as totalTimeSpent from global_temp.finalStream group by userId,logMonth").show(false);
    //


  }

}