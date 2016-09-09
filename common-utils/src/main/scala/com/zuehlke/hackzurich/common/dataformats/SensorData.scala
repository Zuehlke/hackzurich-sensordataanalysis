package com.zuehlke.hackzurich.common.dataformats

import java.time.LocalDateTime

import org.apache.spark.sql.Row

import scala.util.{Failure, Success, Try}


case class GyrometerReading(deviceid : String, sensortype: String = "Gyro", date : String, x: Double, y: Double, z :  Double)

object SensorData {
  def isOfSensorType(row: Row, sensortype: String) = {
    Try (row.getAs[String]("type")) match {
      case Success(rowsensortype) =>   rowsensortype == sensortype
      case Failure(f)             =>   false
    }
  }
}
