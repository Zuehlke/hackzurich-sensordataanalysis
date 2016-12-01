package com.zuehlke.hackzurich.common.dataformats

import org.apache.spark.sql.Row

import scala.util.{Failure, Success, Try}

class SensorTypeFilterJSON4S(sensortype : String) extends Serializable {
  import org.json4s._

  def apply(row: JValue) : Boolean = {
    implicit val formats = DefaultFormats
    Try ((row \ ("type")).extract[String]) match {
      case Success(rowsensortype) =>   rowsensortype == sensortype
      case Failure(f)             =>   println("Failed determine type: "+f); false
    }
  }

  def apply(tuple: Tuple2[String, JValue]) : Boolean  = {
    apply(tuple._2)
  }
}