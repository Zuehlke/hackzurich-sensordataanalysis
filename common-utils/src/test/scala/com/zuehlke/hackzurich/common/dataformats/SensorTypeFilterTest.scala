package com.zuehlke.hackzurich.common.dataformats

import org.json4s.DefaultFormats
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec

@RunWith(classOf[JUnitRunner])
class SensorTypeFilterTest extends FlatSpec {
  implicit val formats = DefaultFormats

  it should "should filter ignore entries without type" in {
    val testDataString = """[{"z" : -0.1, "x" : -0.2, "y" : 0.1, "date" : "1.2.2016"}]"""
    val parsedJSON = SensorReadingJSON4SParser.parseWithJson4s(testDataString)
    val accelerometerFilter = new SensorTypeFilter("ACCELEROMETER")

    val filtered = parsedJSON.filter(accelerometerFilter(_))

    assert(filtered.size == 0)
  }

  it should "should filter specific type" in {
    val testDataString = """[{"z" : -0.1, "x" : -0.2, "y" : 0.1, "date" : "1.2.2016", "type" : "Gyro"},{"z" : 1, "x" : 2, "y" : 3, "date" : "3.3.2016", "type" : "Accelerometer"}]"""
    val parsedJSON = SensorReadingJSON4SParser.parseWithJson4s(testDataString)
    val accelerometerFilter = new SensorTypeFilter("Accelerometer")

    val filtered = parsedJSON.filter(accelerometerFilter(_))

    assert(filtered.size == 1)
    assert(filtered.exists(item => (item \ "type").extract[String] == "Accelerometer"))
    assert(filtered.exists(item => (item \ "date").extract[String] == "3.3.2016"))
  }

  it should "should filter specific type case sensitive" in {
    val testDataString = """[{"z" : -0.1, "x" : -0.2, "y" : 0.1, "date" : "1.2.2016", "type" : "Gyro"},{"z" : 1, "x" : 2, "y" : 3, "date" : "3.3.2016", "type" : "Accelerometer"}]"""
    val parsedJSON = SensorReadingJSON4SParser.parseWithJson4s(testDataString)
    val accelerometerFilter = new SensorTypeFilter("ACCELEROMETER")

    val filtered = parsedJSON.filter(accelerometerFilter(_))

    assert(filtered.size == 0)
  }
}