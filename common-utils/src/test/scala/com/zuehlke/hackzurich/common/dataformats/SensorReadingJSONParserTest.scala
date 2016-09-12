package com.zuehlke.hackzurich.common.dataformats

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.junit.JUnitRunner


/**
  * Test for the JSON parser.
  * Expand to fix problematic cases found in actual data.
  */
@RunWith(classOf[JUnitRunner])
class SensorReadingJSONParserTest extends FlatSpec with Matchers {

  "The JSON parser" should "parse single JSON string" in {
    val singleGyroJsonObject = "{\"z\" : -0.1, \"x\" : -0.2, \"y\" : 0.1, \"date\" : \"2016-09-03T08:40:17.552+02:00\", \"type\" : \"Gyro\"}"

    val parsed = SensorReadingJSONParser.parseReadingsUsingScalaJSONParser(singleGyroJsonObject)
    assert(parsed.size == 1)
    assert(-0.2 == parsed(0).get("x").get)
    assert(0.1 == parsed(0).get("y").get)
    assert(-0.1 == parsed(0).get("z").get)
    assert("Gyro".equals(parsed(0).get("type").get))
  }

  it should "not be bothered by line-breaks" in {
    val singleGyroJsonObject = "  {\n    \"z\" : -0.003133475338587232,\n    \"x\" : -0.06178427202540229,\n    \"y\" : 0.07116925170684153,\n    \"date\" : \"2016-09-03T08:40:17.552+02:00\",\n    \"type\" : \"Gyro\"\n  }"

    val parsed = SensorReadingJSONParser.parseReadingsUsingScalaJSONParser(singleGyroJsonObject)
    assert(parsed.size == 1)
    assert("Gyro".equals(parsed(0).get("type").get))
  }

  it should "parse a JSON array with two elements" in {
    val twoJsonObjects = "[{\"z\" : -0.1, \"x\" : -0.2, \"y\" : 0.1, \"date\" : \"2016-09-03T08:40:17.552+02:00\", \"type\" : \"Gyro\"}, " +
      "{\"z\" : -0.1, \"x\" : -0.2, \"y\" : 0.1, \"date\" : \"2016-09-03T08:40:17.553+02:00\", \"type\" : \"Gyro\"}]"

    val parsed = SensorReadingJSONParser.parseReadingsUsingScalaJSONParser(twoJsonObjects)
    assert(2 == parsed.size)
    assert("2016-09-03T08:40:17.552+02:00".equals(parsed(0).get("date").get))
    assert("2016-09-03T08:40:17.553+02:00".equals(parsed(1).get("date").get))
  }

  it should "parse an empty JSON array" in {
    val twoJsonObjects = "[]"

    val parsed = SensorReadingJSONParser.parseReadingsUsingScalaJSONParser(twoJsonObjects)
    assert(0 == parsed.size)
  }

  it should "not break with Garbage data" in {
    val garbage = "hjciuniunruincoruin"

    val parsed = SensorReadingJSONParser.parseReadingsUsingScalaJSONParser(garbage)
    assert(0 == parsed.size)
  }

  it should "associate the same key to all entries in a ConsumerRecord" in {
    val twoJsonObjects = new ConsumerRecord[String, String]("testopic", 0, 0, "key", "[{\"z\" : -0.1, \"x\" : -0.2, \"y\" : 0.1, \"date\" : \"2016-09-03T08:40:17.552+02:00\", \"type\" : \"Gyro\"}, " +
      "{\"z\" : -0.1, \"x\" : -0.2, \"y\" : 0.1, \"date\" : \"2016-09-03T08:40:17.553+02:00\", \"type\" : \"Gyro\"}]")

    val parsed = SensorReadingJSONParser.parseReadingsUsingScalaJSONParser(twoJsonObjects)
    assert(2 == parsed.size)
    assert("key".equals(parsed(0)._1))
    assert("key".equals(parsed(0)._1))
    assert("2016-09-03T08:40:17.552+02:00".equals(parsed(0)._2.get("date").get))
    assert("2016-09-03T08:40:17.553+02:00".equals(parsed(1)._2.get("date").get))
  }
}