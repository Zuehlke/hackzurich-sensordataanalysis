package com.zuehlke.hackzurich.common.dataformats

import com.sun.rowset.internal.Row
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}

import scala.util.{Failure, Success, Try}
import scala.util.parsing.json.JSON

/**
  * Parses the data submitted by the sensing app,
  * usually indirectly from a Kafka stream.
  */
class SensorReadingJSONParser {

}

object SensorReadingJSONParser {
  def parseUsingJSONParser(unparsedJsonContent : String): List[Map[String, Any]] = {
    val parsed : Option[Any] = JSON.parseFull(unparsedJsonContent)
    parsed match {
      case None => println("No content in: " + unparsedJsonContent); List.empty
      case _ => {
        parsed.get match {
          case entry : Map[String, Any] => List(entry)
          case list : List[Map[String, Any]] => list
          case _ =>  println("Could not parse content in: " + unparsedJsonContent + " / "+ parsed.get.toString); List.empty
        }
      }
    }
  }

  def parseReadingsUsingScalaJSONParser(readings: ConsumerRecord[String, String]): List[Tuple2[String, Map[String, Any]]] = {
    parseReadingsUsingScalaJSONParser(readings.value)
      .map(entry => new Tuple2(readings.key, entry))
  }

  def parseReadingsUsingScalaJSONParser(unparsedJsonContent : String): List[Map[String, Any]] = {
    Try(parseUsingJSONParser(unparsedJsonContent)) match {
      case Success(list) => list
      case Failure(f) => println("Cannot parse data: " + f); List.empty
    }
  }

  def parseUsingSparkSQL(ssql: SQLContext, unparsedJsonContent: RDD[String]): DataFrame = {
    ssql.read.json(unparsedJsonContent)
  }

}
