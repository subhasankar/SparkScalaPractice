package com.panda.spark.demo

import com.panda.spark.demo.App.userSchema
import com.panda.spark.demo.entities.Users

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.spark_partition_id
class StraightPartitioner(p: Int) extends org.apache.spark.Partitioner {
  def numPartitions = p
  def getPartition(key: Any) = key.asInstanceOf[Int]
}
object AttributeScan {
  def main(args: Array[String]): Unit = {
    val spark=SparkUtils.getSparkSession
    import spark.implicits._
    val all=spark.sparkContext.textFile("inputs/att.dat",3)
    all.foreach(a=>{
      println(a);
    })
    val attributes=all.filter(a=>{a.startsWith("A")});
    val allOther=all.filter(a=>{!a.startsWith("A")});

    val repart=allOther.mapPartitionsWithIndex((i,p)=>{
      val arr=p.toArray[String]
      var k=0
            if (i==0 || arr(0).startsWith("C")){
              //nothing to do
            }
            else{
              var done = false

              while ((k <  arr.length) && !done) {
                if (arr(k).startsWith("C")) {
                  done = true
                }
                k+=1
              }
            }
            val overlap = arr.take(k-1)

            val spill = overlap.iterator.map((i - 1, _))

            val keep = arr.drop(k-1).iterator.map((i, _))

            keep ++ spill

    }).partitionBy(new StraightPartitioner(all.partitions.length)).values


    repart.mapPartitions(p => {
      val sVal=p.toSeq
      var all=new ListBuffer[String]
      var custId="Unknown"
      for(i<-0 until sVal.length){
        if(sVal(i).startsWith("C")){
          custId=sVal(i).split(",")(1)
        }
        else{
          all+=custId+","+sVal(i)
        }

      }
      all.iterator
    }).foreach(println(_))

  }
}


