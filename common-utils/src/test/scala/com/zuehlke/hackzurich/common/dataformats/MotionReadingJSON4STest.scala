package com.zuehlke.hackzurich.common.dataformats

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class MotionReadingJSON4STest extends FlatSpec with Matchers {

  "from" should "parse MotionReading json with correct format" in {
    val rawJson = """ {
        "date": "3.3.2016",
        "attitude": {
          "quaternion": {"x": 1.1, "w": 1.2, "y": 2.2, "z": 3.3},
          "rotationMatrix": {"m13": 4.4, "m12": 5.5, "m33": 6.6, "m32": 7.7, "m31": 8.8, "m21": 9.9, "m11": 10.0, "m22": 11.1, "m23": 12.2},
          "pitch": 13.3,
          "yaw": 14.4,
          "roll": 15.5}
        }"""
    val json4s = SensorReadingJSON4SParser.parseWithJson4s(rawJson)
    val tuple = Tuple2("key123", json4s(0))

    val motionReading = MotionReadingJSON4S.from(tuple)

    motionReading.get.sensortype should be ("DeviceMotion")
    motionReading.get.deviceid should be ("key123")
    motionReading.get.date should be ("3.3.2016")
    motionReading.get.x should be (1.1)
    motionReading.get.w should be (1.2)
    motionReading.get.y should be (2.2)
    motionReading.get.z should be (3.3)
    motionReading.get.m13 should be (4.4)
    motionReading.get.m12 should be (5.5)
    motionReading.get.m33 should be (6.6)
    motionReading.get.m32 should be (7.7)
    motionReading.get.m31 should be (8.8)
    motionReading.get.m21 should be (9.9)
    motionReading.get.m11 should be (10.0)
    motionReading.get.m22 should be (11.1)
    motionReading.get.m23 should be (12.2)
  }

  it should "be None for MotionReading json missing field" in {
    val rawJson = """{"date": "3.3.2016"}"""
    val json4s = SensorReadingJSON4SParser.parseWithJson4s(rawJson)
    val tuple = Tuple2("key123", json4s(0))

    val motionReading = MotionReadingJSON4S.from(tuple)

    motionReading should be (None)
  }

  it should "be None for MotionReading json being in wrong format with invalid map" in {
    val rawJson = """{"date": "3.3.2016", "attitude": "notAMap"}"""
    val json4s = SensorReadingJSON4SParser.parseWithJson4s(rawJson)
    val tuple = Tuple2("key123", json4s(0))

    val motionReading = MotionReadingJSON4S.from(tuple)

    motionReading should be (None)
  }

  it should "be None for MotionReading json being in wrong format with invalid inner map" in {
    val rawJson = """{
        "date": "3.3.2016",
        "attitude": {
          "quaternion": {"x": "notADouble", "w": 1.2, "y": 2.2, "z": 3.3},
          "rotationMatrix": {"m13": 4.4, "m12": 5.5, "m33": 6.6, "m32": 7.7, "m31": 8.8, "m21": 9.9, "m11": 10.0, "m22": 11.1, "m23": 12.2},
          "pitch": 13.3,
          "yaw": 14.4,
          "roll": 15.5}
        }"""
    val json4s = SensorReadingJSON4SParser.parseWithJson4s(rawJson)
    val tuple = Tuple2("key123", json4s(0))

    val motionReading = MotionReadingJSON4S.from(tuple)

    motionReading should be (None)
  }
}