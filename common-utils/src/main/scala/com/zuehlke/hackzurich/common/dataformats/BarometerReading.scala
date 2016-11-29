package com.zuehlke.hackzurich.common.dataformats

import org.apache.log4j.LogManager

import scala.util.control.NonFatal

// Hint: Use only lowercase characters in case classes to avoid trouble when storing data in Cassandra,
// where rules for column names my be different

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#barometer */
case class BarometerReading(deviceid : String, date : String, relativealtitude: Double, pressure: Double, sensortype: String = "Barometer")
object BarometerReading {
  def from(t: (String, Map[String,Any])): Option[BarometerReading] = {
    try Some(BarometerReading(
      t._1,
      t._2("date").asInstanceOf[String],
      t._2("relativeAltitude").asInstanceOf[Double],
      t._2("pressure").asInstanceOf[Double]))
    catch {
      case NonFatal(e) => LogManager.getLogger(BarometerReading.getClass).warn("Failed to get data from json. Possible wrong format: " + e); None
    }
  }
}