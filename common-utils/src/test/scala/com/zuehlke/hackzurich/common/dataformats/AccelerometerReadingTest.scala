package com.zuehlke.hackzurich.common.dataformats

import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccelerometerReadingTest extends FlatSpec with Matchers {

  "from" should "parse AccelerometerReading json with correct format" in {
    val tuple = Tuple2("key123", Map("date" -> "3.3.2016", "x" -> 3.3, "y" -> 1.1, "z" -> 4.1))

    val accelerometerReading = AccelerometerReading.from(tuple)

    accelerometerReading.get.sensortype should be ("Accelerometer")
    accelerometerReading.get.deviceid should be ("key123")
    accelerometerReading.get.date should be ("3.3.2016")
    accelerometerReading.get.x should be (3.3)
    accelerometerReading.get.y should be (1.1)
    accelerometerReading.get.z should be (4.1)
  }

  it should "be None for AccelerometerReading json missing field" in {
    val tuple = Tuple2("key123", Map("deas" -> "asd", "x" -> 3.3, "y" -> 1.1, "z" -> 4.1))

    val accelerometerReading = AccelerometerReading.from(tuple)

    accelerometerReading should be (None)
  }

  it should "be None for AccelerometerReading json being in wrong format" in {
    val tuple = Tuple2("key123", Map("date" -> "3.3.2016", "x" -> "notADouble", "y" -> 1.1, "z" -> 4.1))

    val accelerometerReading = AccelerometerReading.from(tuple)

    accelerometerReading should be (None)
  }
}