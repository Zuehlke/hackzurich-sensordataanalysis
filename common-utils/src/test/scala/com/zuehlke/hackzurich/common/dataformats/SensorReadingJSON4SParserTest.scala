package com.zuehlke.hackzurich.common.dataformats

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.json4s.DefaultFormats
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner


/**
  * Test for the JSON parser.
  * Expand to fix problematic cases found in actual data.
  */
@RunWith(classOf[JUnitRunner])
class SensorReadingJSON4SParserTest extends FlatSpec {
  import org.json4s._
  implicit val formats = DefaultFormats

  "The JSON parser" should "parse single JSON string" in {
    val singleGyroJsonObject = "{\"z\" : -0.1, \"x\" : -0.2, \"y\" : 0.1, \"date\" : \"2016-09-03T08:40:17.552+02:00\", \"type\" : \"Gyro\"}"

    val parsed = SensorReadingJSON4SParser.parseWithJson4s(singleGyroJsonObject)
    assert(parsed.size == 1)
    assert(-0.2 == (parsed(0) \ "x").extract[Double])
    assert(0.1 == (parsed(0) \ "y").extract[Double])
    assert(-0.1 == (parsed(0) \ "z").extract[Double])
    assert("Gyro".equals((parsed(0) \ "type").extract[String]))
  }

  it should "not be bothered by line-breaks" in {
    val singleGyroJsonObject = "  {\n    \"z\" : -0.003133475338587232,\n    \"x\" : -0.06178427202540229,\n    \"y\" : 0.07116925170684153,\n    \"date\" : \"2016-09-03T08:40:17.552+02:00\",\n    \"type\" : \"Gyro\"\n  }"

    val parsed = SensorReadingJSON4SParser.parseWithJson4s(singleGyroJsonObject)
    assert(parsed.size == 1)
    assert("Gyro".equals((parsed(0) \ "type").extract[String]))
  }

  it should "parse a JSON array with two elements" in {
    val twoJsonObjects = "[{\"z\" : -0.1, \"x\" : -0.2, \"y\" : 0.1, \"date\" : \"2016-09-03T08:40:17.552+02:00\", \"type\" : \"Gyro\"}, " +
      "{\"z\" : -0.1, \"x\" : -0.2, \"y\" : 0.1, \"date\" : \"2016-09-03T08:40:17.553+02:00\", \"type\" : \"Gyro\"}]"

    val parsed = SensorReadingJSON4SParser.parseWithJson4s(twoJsonObjects)
    assert(2 == parsed.size)
    assert("2016-09-03T08:40:17.552+02:00".equals((parsed(0) \ "date").extract[String]))
    assert("2016-09-03T08:40:17.553+02:00".equals((parsed(1) \ "date").extract[String]))
  }

  it should "parse an empty JSON array" in {
    val twoJsonObjects = "[]"

    val parsed = SensorReadingJSON4SParser.parseWithJson4s(twoJsonObjects)
    assert(0 == parsed.size)
  }

  it should "not break with Garbage data" in {
    val garbage = "hjciuniunruincoruin"

    val parsed = SensorReadingJSON4SParser.parseWithJson4s(garbage)
    assert(0 == parsed.size)
  }

  it should "associate the same key to all entries in a ConsumerRecord" in {
    val twoJsonObjects = new ConsumerRecord[String, String]("testopic", 0, 0, "key", "[{\"z\" : -0.1, \"x\" : -0.2, \"y\" : 0.1, \"date\" : \"2016-09-03T08:40:17.552+02:00\", \"type\" : \"Gyro\"}, " +
      "{\"z\" : -0.1, \"x\" : -0.2, \"y\" : 0.1, \"date\" : \"2016-09-03T08:40:17.553+02:00\", \"type\" : \"Gyro\"}]")

    val parsed = SensorReadingJSON4SParser.parseWithJson4s(twoJsonObjects)
    assert(2 == parsed.size)
    assert("key".equals(parsed(0)._1))
    assert("key".equals(parsed(0)._1))
    assert("2016-09-03T08:40:17.552+02:00".equals((parsed(0)._2 \ "date").extract[String]))
    assert("2016-09-03T08:40:17.553+02:00".equals((parsed(1)._2 \ "date").extract[String]))
  }

  "The JSON parser" should "parse single nested JSON string" in {
    val singleNestedMotionJsonObject = "{\"quaternion\" : { \"x\" : -0.1, \"w\" : 0.2, \"y\" : 0.3, \"z\" : -1.2 }, \"rotationMatrix\" : {\"m13\" : -0.3, \"m12\" : 0.7}, \"pitch\" : -0.8, \"date\" : \"2016-09-03T08:40:17.552+02:00\", \"type\" : \"DeviceMotion\"}"

    val parsed = SensorReadingJSON4SParser.parseWithJson4s(singleNestedMotionJsonObject)
    assert(parsed.size == 1)
    assert(-0.1 == (parsed.head \ "quaternion" \ "x").extract[Double])
    assert(0.2 == (parsed.head \ "quaternion" \ "w").extract[Double])
    assert(0.3 == (parsed.head \ "quaternion" \ "y").extract[Double])
    assert(-1.2 == (parsed.head \ "quaternion" \ "z").extract[Double])
    assert(-0.3 == (parsed.head \ "rotationMatrix" \ "m13").extract[Double])
    assert(0.7 == (parsed.head \ "rotationMatrix" \ "m12").extract[Double])
    assert(-0.8 == (parsed.head \ "pitch").extract[Double]) //same as parsed(0).get("pitch").get
    assert("DeviceMotion".equals((parsed.head \"type").extract[String]))
  }

  "The JSON parser" should "parse double nested JSON string" in {
    val singleNestedMotionJsonObject = "{\"attitude\" : {\"quaternion\" : { \"x\" : -0.1, \"w\" : 0.2, \"y\" : 0.3, \"z\" : -1.2 }, \"rotationMatrix\" : {\"m13\" : -0.3, \"m12\" : 0.7}, \"pitch\" : -0.8}, \"date\" : \"2016-09-03T08:40:17.552+02:00\", \"type\" : \"DeviceMotion\"}"

    val parsed = SensorReadingJSON4SParser.parseWithJson4s(singleNestedMotionJsonObject)
    assert(parsed.size == 1)
    assert(-0.1 == (parsed(0) \ "attitude" \ "quaternion" \ "x").extract[Double])
    assert("DeviceMotion".equals((parsed(0) \ "type").extract[String]))
  }
}