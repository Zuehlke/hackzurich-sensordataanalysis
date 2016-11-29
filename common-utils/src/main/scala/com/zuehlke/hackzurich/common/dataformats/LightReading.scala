package com.zuehlke.hackzurich.common.dataformats

import org.apache.log4j.LogManager

import scala.util.control.NonFatal

// Hint: Use only lowercase characters in case classes to avoid trouble when storing data in Cassandra,
// where rules for column names my be different

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#light */
case class LightReading(deviceid : String, date : String, brightnes: Double, sensortype: String = "Light")
object LightReading {
  def from(t: (String, Map[String,Any])): Option[LightReading] = {
    try Some(LightReading(
      t._1,
      t._2("date").asInstanceOf[String],
      t._2("brightnes").asInstanceOf[Double]))
    catch {
      case NonFatal(e) => LogManager.getLogger(LightReading.getClass).warn("Failed to get data from json. Possible wrong format: " + e); None
    }
  }
}