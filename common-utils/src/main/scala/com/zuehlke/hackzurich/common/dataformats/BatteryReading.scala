package com.zuehlke.hackzurich.common.dataformats

import org.apache.log4j.LogManager

import scala.util.control.NonFatal

// Hint: Use only lowercase characters in case classes to avoid trouble when storing data in Cassandra,
// where rules for column names my be different

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#batterylevel */
case class BatteryReading(deviceid : String, date : String, batterystate: String, batterylevel: Double, sensortype: String = "Battery")
object BatteryReading {
  def from(t: (String, Map[String,Any])): Option[BatteryReading] = {
    try Some(BatteryReading(
      t._1,
      t._2("date").asInstanceOf[String],
      t._2("batteryState").asInstanceOf[String],
      t._2("batteryLevel").asInstanceOf[Double]))
    catch {
      case NonFatal(e) => LogManager.getLogger(BatteryReading.getClass).warn("Failed to get data from json. Possible wrong format: " + e); None
    }
  }
}