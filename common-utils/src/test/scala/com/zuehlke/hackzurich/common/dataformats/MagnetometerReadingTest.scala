package com.zuehlke.hackzurich.common.dataformats

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class MagnetometerReadingTest extends FlatSpec with Matchers {

  "from" should "parse MagnetometerReading json with correct format" in {
    val tuple = Tuple2("key123", Map("date" -> "3.3.2016", "x" -> 1.1, "y" -> 2.2, "z" -> 3.3))

    val magnetometerReading = MagnetometerReading.from(tuple)

    magnetometerReading.get.sensortype should be ("Magnetometer")
    magnetometerReading.get.deviceid should be ("key123")
    magnetometerReading.get.date should be ("3.3.2016")
    magnetometerReading.get.x should be (1.1)
    magnetometerReading.get.y should be (2.2)
    magnetometerReading.get.z should be (3.3)
  }

  it should "be None for MagnetometerReading json missing field" in {
    val tuple = Tuple2("key123", Map("date" -> "asd", "y" -> 2.2, "z" -> 3.3))

    val magnetometerReading = MagnetometerReading.from(tuple)

    magnetometerReading should be (None)
  }

  it should "be None for MagnetometerReading json being in wrong format" in {
    val tuple = Tuple2("key123", Map("date" -> "3.3.2016", "x" -> "notADouble", "y" -> 2.2, "z" -> 3.3))

    val magnetometerReading = MagnetometerReading.from(tuple)

    magnetometerReading should be (None)
  }
}