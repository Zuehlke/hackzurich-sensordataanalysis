package com.zuehlke.hackzurich.common.dataformats

import org.apache.log4j.LogManager

import scala.util.control.NonFatal

// Hint: Use only lowercase characters in case classes to avoid trouble when storing data in Cassandra,
// where rules for column names my be different

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#devicemotion */
case class MotionReadingJSON4S(deviceid : String, date : String, x: Double, w: Double, y: Double, z:  Double, m13: Double, m12: Double, m33: Double, m32: Double, m31: Double, m21: Double, m11: Double, m22: Double, m23: Double, pitch: Double, sensortype: String = "DeviceMotion")
object MotionReadingJSON4S {
  import org.json4s._
  def from(t: (String, JValue)): Option[MotionReadingJSON4S] = {
    implicit val formats = DefaultFormats
    try Some(MotionReadingJSON4S(
      t._1,
      (t._2 \ "date").extract[String],
      (t._2 \ "attitude" \ "quaternion" \ "x").extract[Double],
      (t._2 \ "attitude" \ "quaternion" \"w").extract[Double],
      (t._2 \ "attitude" \ "quaternion" \ "y").extract[Double],
      (t._2 \ "attitude" \ "quaternion" \ "z").extract[Double],
      (t._2 \ "attitude" \ "rotationMatrix" \ "m13").extract[Double],
      (t._2 \ "attitude" \ "rotationMatrix" \ "m12").extract[Double],
      (t._2 \ "attitude" \ "rotationMatrix" \ "m33").extract[Double],
      (t._2 \ "attitude" \ "rotationMatrix" \ "m32").extract[Double],
      (t._2 \ "attitude" \ "rotationMatrix" \ "m31").extract[Double],
      (t._2 \ "attitude" \ "rotationMatrix" \ "m21").extract[Double],
      (t._2 \ "attitude" \ "rotationMatrix" \ "m11").extract[Double],
      (t._2 \ "attitude" \ "rotationMatrix" \ "m22").extract[Double],
      (t._2 \ "attitude" \ "rotationMatrix" \ "m23").extract[Double],
      (t._2 \ "attitude" \ "pitch").extract[Double]))
    catch {
      case NonFatal(e) => LogManager.getLogger(MotionReadingJSON4S.getClass).warn("Failed to get data from json. Possible wrong format: " + e); None
    }
  }
}