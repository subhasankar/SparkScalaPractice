package com.panda.spark.demo

import java.text.SimpleDateFormat

import org.apache.spark.sql.types._
case class trip(id :Int,count:Int)
object TripCounts {
  val format =new SimpleDateFormat("MM/dd/yyyy")
  def main(args: Array[String]): Unit = {

    val spark=SparkUtils.getSparkSession
    import spark.implicits._
    val trips=spark.read.schema(tripSchema).format("csv").load("inputs/trips.dat").select("customerId","tripDate")
    trips.groupByKey(r=>r.getInt(0)).mapGroups((a,b)=>{
      val values=b.toList.map(d=>{(d.getInt(0),format.parse(d.getString(1)).getTime)}).sortWith((d1,d2)=>d1._2<d2._2)
      var minDate=values(0)._2
      var count=1
      values.foreach(v=>{
        val curDate=v._2
          if((curDate-minDate)>7*24*60*60*1000){
            minDate=curDate
            count=count+1;
          }
      })
      trip(a,count)
    }).write.save("outputs/test/")
   // System.in.read

    //spark.stop
  }
  def tripSchema :StructType={
    StructType(
      Seq(
        StructField("customerId",IntegerType,false),
        StructField("fname",StringType,false),
        StructField("lname",StringType,false),
        StructField("gender",StringType,false),
        StructField("tripDate",StringType,false)
      )
    )
  }
}
