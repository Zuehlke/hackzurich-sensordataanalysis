package com.zuehlke.hackzurich.common.dataformats

import org.apache.log4j.LogManager

import scala.util.control.NonFatal

// Hint: Use only lowercase characters in case classes to avoid trouble when storing data in Cassandra,
// where rules for column names my be different

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#microphone */
case class MicrophoneReading(deviceid : String, date : String,  peakpower: Double, averagepower: Double, sensortype: String = "Microphone")
object MicrophoneReading {
  def from(t: (String, Map[String,Any])): Option[MicrophoneReading] = {
    try Some(MicrophoneReading(
      t._1,
      t._2("date").asInstanceOf[String],
      t._2("peakPower").asInstanceOf[Double],
      t._2("averagePower").asInstanceOf[Double]))
    catch {
      case NonFatal(e) => LogManager.getLogger(MicrophoneReading.getClass).warn("Failed to get data from json. Possible wrong format: " + e); None
    }
  }
}