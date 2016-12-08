package com.zuehlke.hackzurich.common.dataformats

import org.apache.spark.sql.Row

import scala.util.{Failure, Success, Try}

class SensorTypeFilter(sensortype : String) extends Serializable {
  def apply(row: Row) : Boolean  = {
    Try (row.getAs[String]("type")) match {
      case Success(rowsensortype) =>   rowsensortype == sensortype
      case Failure(f)             =>   println("Failed determine type: "+f); false
    }
  }

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