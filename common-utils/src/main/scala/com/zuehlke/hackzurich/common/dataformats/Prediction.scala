package com.zuehlke.hackzurich.common.dataformats

import java.text.SimpleDateFormat

import scala.collection.mutable.ArrayBuffer


case class Prediction(timestamp: Long, deviceid: String, values: Seq[Double]) {
  private val timeString = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestamp)

  def toCsv: String = {
    s"$timestamp ; $deviceid ; ${values.mkString(" ; ")}"
  }

  override def toString: String = {
    s"$timeString $deviceid [${values.mkString(" , ")}]"
  }
}

object Prediction {
  def apply(csv: String): Prediction = {
    try {
      val split = csv.split(";").map(_.trim)
      val _timestamp = split(0).toLong
      val _deviceid = split(1)
      val _values = new ArrayBuffer[Double]()
      for (v <- split.slice(2, split.length)) {
        _values += v.toDouble
      }
      Prediction(_timestamp, _deviceid, _values)
    } catch {
      case e: Exception => Prediction(-1, e.getMessage, Seq.empty[Double])
    }
  }
}