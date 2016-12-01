package com.zuehlke.hackzurich.common.dataformats

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class GyrometerReadingJSON4STest extends FlatSpec with Matchers {

  "from" should "parse GyrometerReading json with correct format" in {
    val rawJson = """{"date": "3.3.2016", "x": 1.1, "y": 2.2, "z": 3.3}"""
    val json4s = SensorReadingJSON4SParser.parseWithJson4s(rawJson)
    val tuple = Tuple2("key123", json4s(0))

    val gyrometerReading = GyrometerReadingJSON4S.from(tuple)

    gyrometerReading.get.sensortype should be ("Gyro")
    gyrometerReading.get.deviceid should be ("key123")
    gyrometerReading.get.date should be ("3.3.2016")
    gyrometerReading.get.x should be (1.1)
    gyrometerReading.get.y should be (2.2)
    gyrometerReading.get.z should be (3.3)
  }

  it should "be None for GyrometerReading json missing field" in {
    val rawJson = """{"date": "asd", "y": 2.2, "z": 3.3}"""
    val json4s = SensorReadingJSON4SParser.parseWithJson4s(rawJson)
    val tuple = Tuple2("key123", json4s(0))

    val gyrometerReading = GyrometerReadingJSON4S.from(tuple)

    gyrometerReading should be (None)
  }

  it should "be None for GyrometerReading json being in wrong format" in {
    val rawJson = """{"date": "3.3.2016", "x": "notADouble", "y": 2.2, "z": 3.3}"""
    val json4s = SensorReadingJSON4SParser.parseWithJson4s(rawJson)
    val tuple = Tuple2("key123", json4s(0))

    val gyrometerReading = GyrometerReadingJSON4S.from(tuple)

    gyrometerReading should be (None)
  }
}