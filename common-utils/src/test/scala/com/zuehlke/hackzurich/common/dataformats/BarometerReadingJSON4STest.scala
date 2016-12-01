package com.zuehlke.hackzurich.common.dataformats

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class BarometerReadingJSON4STest extends FlatSpec with Matchers {

  "from" should "parse BarometerReading json with correct format" in {
    val rawJson = """{"date": "3.3.2016", "relativeAltitude": 4.5, "pressure": 123.1}"""
    val json4s = SensorReadingJSON4SParser.parseWithJson4s(rawJson)
    val tuple = Tuple2("key123", json4s(0))

    val barometerReading = BarometerReadingJSON4S.from(tuple)

    barometerReading.get.sensortype should be ("Barometer")
    barometerReading.get.deviceid should be ("key123")
    barometerReading.get.date should be ("3.3.2016")
    barometerReading.get.relativealtitude should be (4.5)
    barometerReading.get.pressure should be (123.1)
  }

  it should "be None for BarometerReading json missing field" in {
    val rawJson = """{"deas": "asd", "x": 3.3, "y": 1.1, "z": 4.1}"""
    val json4s = SensorReadingJSON4SParser.parseWithJson4s(rawJson)
    val tuple = Tuple2("key123", json4s(0))

    val barometerReading = BarometerReadingJSON4S.from(tuple)

    barometerReading should be (None)
  }

  it should "be None for BarometerReading json being in wrong format" in {
    val rawJson = """{"date": "3.3.2016", "relativeAltitude": "invalidFormat", "pressure": 123.1}"""
    val json4s = SensorReadingJSON4SParser.parseWithJson4s(rawJson)
    val tuple = Tuple2("key123", json4s(0))

    val barometerReading = BarometerReadingJSON4S.from(tuple)

    barometerReading should be (None)
  }
}