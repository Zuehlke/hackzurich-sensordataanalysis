package com.zuehlke.hackzurich.common.dataformats

import org.apache.log4j.LogManager

import scala.util.control.NonFatal

// Hint: Use only lowercase characters in case classes to avoid trouble when storing data in Cassandra,
// where rules for column names my be different

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#batterylevel */
case class BatteryReadingJSON4S(deviceid : String, date : String, batterystate: String, batterylevel: Double, sensortype: String = "Battery")
object BatteryReadingJSON4S {
  import org.json4s._
  def from(t: (String, JValue)): Option[BatteryReadingJSON4S] = {
    implicit val formats = DefaultFormats
    try Some(BatteryReadingJSON4S(
      t._1,
      (t._2 \ "date").extract[String],
      (t._2 \ "batteryState").extract[String],
      (t._2 \ "batteryLevel").extract[Double]))
    catch {
      case NonFatal(e) => LogManager.getLogger(BatteryReadingJSON4S.getClass).warn("Failed to get data from json. Possible wrong format: " + e); None
    }
  }
}