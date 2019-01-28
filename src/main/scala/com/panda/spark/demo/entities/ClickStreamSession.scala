package com.panda.spark.demo.entities

import java.sql.Timestamp

case class ClickStreamSession(userId: String, logTime: Timestamp, timeSpent:Integer,sessionId:String, logMonth:String,logDate:String)
