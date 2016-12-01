package com.zuehlke.hackzurich.common.dataformats

import org.apache.log4j.LogManager
import scala.util.control.NonFatal

// Hint: Use only lowercase characters in case classes to avoid trouble when storing data in Cassandra,
// where rules for column names my be different

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#accelerometer */
case class AccelerometerReadingJSON4S(deviceid : String, date : String, x: Double, y: Double, z :  Double, sensortype: String = "Accelerometer")

object AccelerometerReadingJSON4S {
  import org.json4s._
  def from(t: (String, JValue)): Option[AccelerometerReadingJSON4S] = {
    implicit val formats = DefaultFormats
    try Some(AccelerometerReadingJSON4S(
      t._1,
      (t._2 \ "date").extract[String],
      (t._2 \ "x").extract[Double],
      (t._2 \ "y").extract[Double],
      (t._2 \ "z").extract[Double]))
    catch {
      case NonFatal(e) => LogManager.getLogger(AccelerometerReadingJSON4S.getClass).warn("Failed to get data from json. Possible wrong format: " + e); None
    }
  }
}