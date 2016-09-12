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

  def apply(row: Map[String, Any]) : Boolean = {
    Try (row.get("type").asInstanceOf[Some[Any]]) match {
      case Success(rowsensortype) =>   rowsensortype.get == sensortype
      case Failure(f)             =>   println("Failed determine type: "+f); false
    }
  }

  def apply(tuple: Tuple2[String, Map[String, Any]]) : Boolean  = {
    apply(tuple._2)
  }
}