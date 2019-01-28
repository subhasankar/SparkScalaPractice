package com.panda.spark.demo.entities

import java.sql.Timestamp

case class ClickStream(logTime: Timestamp, userId: String)

