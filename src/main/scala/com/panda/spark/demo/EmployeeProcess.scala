package com.panda.spark.demo

import com.panda.spark.demo.TripCounts.tripSchema
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
case class EmployeePr(employeeId:Int,name:String,department:String,salary:Int,maxSal:Int)
object EmployeeProcess {
  def main(args: Array[String]): Unit = {
    val spark=SparkUtils.getSparkSession
    import spark.implicits._
    val employees=spark.read.schema(tripSchema).format("csv").load("inputs/employees.dat")
    //employees.withColumn("maxSal",max("salary").over(Window.partitionBy("department"))).show()

    //employees.createOrReplaceGlobalTempView("employees")
    //spark.sql("select employeeId,name,department,salary,max(salary) over (partition by department) as maxSal from global_temp.employees").show(false)

    val maxData=spark.sparkContext.broadcast(employees.groupBy("department").max("salary").map(a=>(a.getString(0),a.getInt(1))).collect.toMap[String,Int])
    employees.map(r=>{
      EmployeePr(r.getInt(0),r.getString(1),r.getString(2),r.getInt(3),maxData.value.getOrElse(r.getString(2),0))
    }).as[EmployeePr].show(false)

  }

  def tripSchema :StructType={
    StructType(
      Seq(
        StructField("employeeId",IntegerType,false),
        StructField("name",StringType,false),
        StructField("departMent",StringType,false),
        StructField("salary",IntegerType,false)
      )
    )
  }
}
