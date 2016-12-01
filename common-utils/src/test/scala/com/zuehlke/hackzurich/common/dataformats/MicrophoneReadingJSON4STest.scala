package com.zuehlke.hackzurich.common.dataformats

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class MicrophoneReadingJSON4STest extends FlatSpec with Matchers {

  "from" should "parse MicrophoneReading json with correct format" in {
    val rawJson = """{"date": "3.3.2016", "peakPower": 1.1, "averagePower": 2.2}"""
    val json4s = SensorReadingJSON4SParser.parseWithJson4s(rawJson)
    val tuple = Tuple2("key123", json4s(0))

    val microphoneReading = MicrophoneReadingJSON4S.from(tuple)

    microphoneReading.get.sensortype should be ("Microphone")
    microphoneReading.get.deviceid should be ("key123")
    microphoneReading.get.date should be ("3.3.2016")
    microphoneReading.get.peakpower should be (1.1)
    microphoneReading.get.averagepower should be (2.2)
  }

  it should "be None for MicrophoneReading json missing field" in {
    val rawJson = """{"date": "3.3.2016"}"""
    val json4s = SensorReadingJSON4SParser.parseWithJson4s(rawJson)
    val tuple = Tuple2("key123", json4s(0))

    val microphoneReading = MicrophoneReadingJSON4S.from(tuple)

    microphoneReading should be (None)
  }

  it should "be None for MicrophoneReading json being in wrong format" in {
    val rawJson = """{"date": "3.3.2016", "peakPower": "notADouble", "averagePower": 2.2}"""
    val json4s = SensorReadingJSON4SParser.parseWithJson4s(rawJson)
    val tuple = Tuple2("key123", json4s(0))

    val microphoneReading = MicrophoneReadingJSON4S.from(tuple)

    microphoneReading should be (None)
  }
}