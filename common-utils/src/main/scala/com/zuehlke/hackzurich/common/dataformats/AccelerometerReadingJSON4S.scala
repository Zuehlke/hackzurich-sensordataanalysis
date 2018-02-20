package com.zuehlke.hackzurich.common.dataformats

import org.apache.log4j.LogManager

import scala.util.control.NonFatal

// Hint: Use only lowercase characters in case classes to avoid trouble when storing data in Cassandra,
// where rules for column names my be different

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#accelerometer */
case class AccelerometerReadingJSON4S(deviceid: String, date: Long, x: Double, y: Double, z: Double, sensortype: String = "Accelerometer") extends Ordered[AccelerometerReadingJSON4S] {
  def toCsv: String = s"$date ; $deviceid ; $x ; $y ; $z"

  override def compare(that: AccelerometerReadingJSON4S): Int = this.date.compareTo(that.date)
}

object AccelerometerReadingJSON4S {

  import org.json4s._

  def from(t: (String, JValue)): Option[AccelerometerReadingJSON4S] = {
    implicit val formats = DefaultFormats
    try Some(AccelerometerReadingJSON4S(
      t._1,
      System.currentTimeMillis(), // We want to use the current timestamp for being able to simulate "real-time" data to be used in the time-series analysis
      (t._2 \ "x").extract[Double],
      (t._2 \ "y").extract[Double],
      (t._2 \ "z").extract[Double]))
    catch {
      case NonFatal(e) => LogManager.getLogger(AccelerometerReadingJSON4S.getClass).warn("Failed to get data from json. Possible wrong format: " + e); None
    }
  }

  def apply(csv: String): AccelerometerReadingJSON4S = {
    try {
      val split = csv.split(";").map(_.trim)
      val _timestamp = split(0).toLong
      val _deviceid = split(1)
      val _x = split(2).toDouble
      val _y = split(3).toDouble
      val _z = split(4).toDouble

      AccelerometerReadingJSON4S(_deviceid, _timestamp, _x, _y, _z)
    } catch {
      case e: Exception => AccelerometerReadingJSON4S(e.getMessage, -1, -1, -1, -1)
    }
  }
}