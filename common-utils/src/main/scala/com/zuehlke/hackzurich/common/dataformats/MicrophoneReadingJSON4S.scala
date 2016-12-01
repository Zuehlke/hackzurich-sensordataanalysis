package com.zuehlke.hackzurich.common.dataformats

import org.apache.log4j.LogManager

import scala.util.control.NonFatal

// Hint: Use only lowercase characters in case classes to avoid trouble when storing data in Cassandra,
// where rules for column names my be different

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#microphone */
case class MicrophoneReadingJSON4S(deviceid : String, date : String,  peakpower: Double, averagepower: Double, sensortype: String = "Microphone")
object MicrophoneReadingJSON4S {
  import org.json4s._
  def from(t: (String, JValue)): Option[MicrophoneReadingJSON4S] = {
    implicit val formats = DefaultFormats
    try Some(MicrophoneReadingJSON4S(
      t._1,
      (t._2 \ "date").extract[String],
      (t._2 \ "peakPower").extract[Double],
      (t._2 \ "averagePower").extract[Double]))
    catch {
      case NonFatal(e) => LogManager.getLogger(MicrophoneReading.getClass).warn("Failed to get data from json. Possible wrong format: " + e); None
    }
  }
}