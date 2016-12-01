package com.zuehlke.hackzurich.common.dataformats

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class BatteryReadingJSON4STest extends FlatSpec with Matchers {

  "from" should "parse BatteryReading json with correct format" in {
    val rawJson = """{"date": "3.3.2016", "batteryState": "str", "batteryLevel": 100.0}"""
    val json4s = SensorReadingJSON4SParser.parseWithJson4s(rawJson)
    val tuple = Tuple2("key123", json4s(0))


    val batteryReading = BatteryReadingJSON4S.from(tuple)

    batteryReading.get.sensortype should be ("Battery")
    batteryReading.get.deviceid should be ("key123")
    batteryReading.get.date should be ("3.3.2016")
    batteryReading.get.batterystate should be ("str")
    batteryReading.get.batterylevel should be (100.0)
  }

  it should "be None for BatteryReading json missing field" in {
    val rawJson = """{"date": "asd", "batteryState": "str"}"""
    val json4s = SensorReadingJSON4SParser.parseWithJson4s(rawJson)
    val tuple = Tuple2("key123", json4s(0))

    val batteryReading = BatteryReadingJSON4S.from(tuple)

    batteryReading should be (None)
  }

  it should "be None for BatteryReading json being in wrong format" in {
    val rawJson = """{"date": "3.3.2016", "batteryState": "str", "batteryLevel": "invalidDouble"}"""
    val json4s = SensorReadingJSON4SParser.parseWithJson4s(rawJson)
    val tuple = Tuple2("key123", json4s(0))

    val batteryReading = BatteryReadingJSON4S.from(tuple)

    batteryReading should be (None)
  }
}