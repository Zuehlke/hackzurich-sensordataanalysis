package com.zuehlke.hackzurich.common.dataformats

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class LightReadingTest extends FlatSpec with Matchers {

  "from" should "parse LightReading json with correct format" in {
    val tuple = Tuple2("key123", Map("date" -> "3.3.2016", "brightnes" -> 1.1))

    val lightReading = LightReading.from(tuple)

    lightReading.get.sensortype should be ("Light")
    lightReading.get.deviceid should be ("key123")
    lightReading.get.date should be ("3.3.2016")
    lightReading.get.brightnes should be (1.1)
  }

  it should "be None for LightReading json missing field" in {
    val tuple = Tuple2("key123", Map("date" -> "3.3.2016"))

    val lightReading = LightReading.from(tuple)

    lightReading should be (None)
  }

  it should "be None for LightReading json being in wrong format" in {
    val tuple = Tuple2("key123", Map("date" -> "3.3.2016", "brightnes" -> "notADouble"))

    val lightReading = LightReading.from(tuple)

    lightReading should be (None)
  }
}