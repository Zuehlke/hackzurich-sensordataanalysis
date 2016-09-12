package com.zuehlke.hackzurich.common.dataformats

import org.apache.spark.sql.Row

import scala.util.{Failure, Success, Try}

class SensorTypeFilter(sensortype : String) extends Serializable {
  def apply(row: Row) = {
    Try (row.getAs[String]("type")) match {
      case Success(rowsensortype) =>   rowsensortype == sensortype
      case Failure(f)             =>   false
    }
  }
}
