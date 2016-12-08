package com.zuehlke.hackzurich.common.dataformats

import org.apache.log4j.LogManager
import scala.util.control.NonFatal
import org.json4s._

/**
  * Created by dev on 12/1/16.
  */
// Hint: Use only lowercase characters in case classes to avoid trouble when storing data in Cassandra,
// where rules for column names my be different

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#barometer */
case class BarometerReadingJSON4S(deviceid : String, date : String, relativealtitude: Double, pressure: Double, sensortype: String = "Barometer")
object BarometerReadingJSON4S {
  def from(t: (String, JValue)): Option[BarometerReadingJSON4S] = {
    implicit val formats = DefaultFormats
    try Some(BarometerReadingJSON4S(
      t._1,
      (t._2 \ "date").extract[String],
      (t._2 \ "relativeAltitude").extract[Double],
      (t._2 \ "pressure").extract[Double]))
    catch {
      case NonFatal(e) => LogManager.getLogger(BarometerReadingJSON4S.getClass).warn("Failed to get data from json. Possible wrong format: " + e); None
    }
  }
}