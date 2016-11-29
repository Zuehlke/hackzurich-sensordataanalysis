package com.zuehlke.hackzurich.common.dataformats

import org.apache.log4j.LogManager

import scala.util.control.NonFatal

// Hint: Use only lowercase characters in case classes to avoid trouble when storing data in Cassandra,
// where rules for column names my be different

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#gyrometer */
case class GyrometerReading(deviceid : String, date : String, x: Double, y: Double, z:  Double, sensortype: String = "Gyro")
object GyrometerReading {
  def from(t: (String, Map[String,Any])): Option[GyrometerReading] = {
    try Some(GyrometerReading(
      t._1,
      t._2("date").asInstanceOf[String],
      t._2("x").asInstanceOf[Double],
      t._2("y").asInstanceOf[Double],
      t._2("z").asInstanceOf[Double]))
    catch {
      case NonFatal(e) => LogManager.getLogger(GyrometerReading.getClass).warn("Failed to get data from json. Possible wrong format: " + e); None
    }
  }
}
