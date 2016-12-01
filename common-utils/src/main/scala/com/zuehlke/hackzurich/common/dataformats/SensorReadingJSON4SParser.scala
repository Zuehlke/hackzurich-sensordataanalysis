package com.zuehlke.hackzurich.common.dataformats

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.LogManager

import scala.util.{Success, Try}

object SensorReadingJSON4SParser {
  import org.json4s._
  import org.json4s.native.JsonMethods._

  def parseWithJson4s(readings: ConsumerRecord[String, String]): List[Tuple2[String, JValue]] = {
    val value = readings.value
    val wrapList = parseWithJson4s(value)
    wrapList.map(entry => Tuple2(readings.key,entry))
  }

  def parseWithJson4s(unparsed: String) : List[JValue] = {
    val json = Try(parse(unparsed)) match {
      case Success(result) => result;
      case _ => Nil
    }
    json match {
      case o: JObject => List(o)
      case JArray(list) => list
      case _ => LogManager.getLogger(SensorReadingJSON4SParser.getClass).error("failed to read json format: " + unparsed); Nil
    }
  }
}