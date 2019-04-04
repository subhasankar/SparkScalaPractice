package mytest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      appName("spark-runner")
      .config("spark.master", "local")
      .config("spark.driver.host", "localhost")
      .config("spark.submit.deployMode", "client")
      .config("spark.sql.shuffle.partitions", "2")
      //.enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    class StraightPartitioner(p: Int) extends org.apache.spark.Partitioner {
      def numPartitions = p
      def getPartition(key: Any) = key.asInstanceOf[Int]
    }
    val input=spark.sparkContext.parallelize(1 to 100,10)

    val partitioned=input.mapPartitionsWithIndex((i,p)=>{
      val overlap = p.take(3 - 1).toArray
      val spill = overlap.iterator.map((i - 1, _))
      val keep = (overlap.iterator ++ p).map((i, _))
      if (i == 0) keep else keep ++ spill
    }).partitionBy(new StraightPartitioner(input.partitions.length)).values

    val movingAverage = partitioned.mapPartitions(p => {
      val sVal=p.toSeq
         var all=new ListBuffer[Double]
          for(i<-2 until sVal.length){
            all+=(sVal(i)+sVal(i-1)+sVal(i-2))/3
          }
      all.iterator
    }).foreach(x=>println(x))


    System.in.read
    spark.stop
  }
}
