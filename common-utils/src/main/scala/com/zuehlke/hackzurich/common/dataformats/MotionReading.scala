package com.zuehlke.hackzurich.common.dataformats

import scala.util.control.NonFatal

// Hint: Use only lowercase characters in case classes to avoid trouble when storing data in Cassandra,
// where rules for column names my be different

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#devicemotion */
case class MotionReading(deviceid : String, date : String, x: Double, w: Double, y: Double, z:  Double, m13: Double, m12: Double, m33: Double, m32: Double, m31: Double, m21: Double, m11: Double, m22: Double, m23: Double, pitch: Double, sensortype: String = "DeviceMotion")
object MotionReading {
  def from(t: (String, Map[String,Any])): Option[MotionReading] = {
    try Some(MotionReading(
      t._1,
      t._2("date").asInstanceOf[String],
      t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("quaternion").flatMap { case p3: Map[String, _] => p3.get("x")} }.get.asInstanceOf[Double],
      t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("quaternion").flatMap { case p3: Map[String, _] => p3.get("w")} }.get.asInstanceOf[Double],
      t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("quaternion").flatMap { case p3: Map[String, _] => p3.get("y")} }.get.asInstanceOf[Double],
      t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("quaternion").flatMap { case p3: Map[String, _] => p3.get("z")} }.get.asInstanceOf[Double],
      t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("rotationMatrix").flatMap { case p3: Map[String, _] => p3.get("m13")} }.get.asInstanceOf[Double],
      t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("rotationMatrix").flatMap { case p3: Map[String, _] => p3.get("m12")} }.get.asInstanceOf[Double],
      t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("rotationMatrix").flatMap { case p3: Map[String, _] => p3.get("m33")} }.get.asInstanceOf[Double],
      t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("rotationMatrix").flatMap { case p3: Map[String, _] => p3.get("m32")} }.get.asInstanceOf[Double],
      t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("rotationMatrix").flatMap { case p3: Map[String, _] => p3.get("m31")} }.get.asInstanceOf[Double],
      t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("rotationMatrix").flatMap { case p3: Map[String, _] => p3.get("m21")} }.get.asInstanceOf[Double],
      t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("rotationMatrix").flatMap { case p3: Map[String, _] => p3.get("m11")} }.get.asInstanceOf[Double],
      t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("rotationMatrix").flatMap { case p3: Map[String, _] => p3.get("m22")} }.get.asInstanceOf[Double],
      t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("rotationMatrix").flatMap { case p3: Map[String, _] => p3.get("m23")} }.get.asInstanceOf[Double],
      t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("pitch")}.get.asInstanceOf[Double]))
    catch {
      case NonFatal(e) => println("Failed to get data from json. Possible wrong format: " + e); None
    }
  }
}