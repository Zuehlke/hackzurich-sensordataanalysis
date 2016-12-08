package com.zuehlke.hackzurich.common.dataformats

import org.apache.log4j.LogManager

import scala.util.control.NonFatal

// Hint: Use only lowercase characters in case classes to avoid trouble when storing data in Cassandra,
// where rules for column names my be different

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#magnetometer */
case class MagnetometerReadingJSON4S(deviceid : String, date : String, x: Double, y: Double, z:  Double, sensortype: String = "Magnetometer")

object MagnetometerReadingJSON4S {
  import org.json4s._
  def from(t: (String, JValue)): Option[MagnetometerReadingJSON4S] = {
    implicit val formats = DefaultFormats
    try Some(MagnetometerReadingJSON4S(
      t._1,
      (t._2 \ "date").extract[String],
      (t._2 \ "x").extract[Double],
      (t._2 \ "y").extract[Double],
      (t._2 \"z").extract[Double]))
    catch {
      case NonFatal(e) => LogManager.getLogger(MagnetometerReadingJSON4S.getClass).warn("Failed to get data from json. Possible wrong format: " + e); None
    }
  }
}