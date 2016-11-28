package com.zuehlke.hackzurich.common.dataformats

import scala.util.control.NonFatal

// Hint: Use only lowercase characters in case classes to avoid trouble when storing data in Cassandra,
// where rules for column names my be different

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#accelerometer */
case class AccelerometerReading(deviceid : String, date : String, x: Double, y: Double, z :  Double, sensortype: String = "Accelerometer")

object AccelerometerReading {
  def from(t: (String, Map[String,Any])): Option[AccelerometerReading] = {
    try Some(AccelerometerReading(
      t._1,
      t._2("date").asInstanceOf[String],
      t._2("x").asInstanceOf[Double],
      t._2("y").asInstanceOf[Double],
      t._2("z").asInstanceOf[Double]))
    catch {
      case NonFatal(e) => println("Failed to get data from json. Possible wrong format: " + e); None
    }
  }
}