package com.zuehlke.hackzurich.common.dataformats

import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BatteryReadingTest extends FlatSpec with Matchers {

  "from" should "parse BatteryReading json with correct format" in {
    val tuple = Tuple2("key123", Map("date" -> "3.3.2016", "batteryState" -> "str", "batteryLevel" -> 100.0))

    val batteryReading = BatteryReading.from(tuple)

    batteryReading.get.sensortype should be ("Battery")
    batteryReading.get.deviceid should be ("key123")
    batteryReading.get.date should be ("3.3.2016")
    batteryReading.get.batterystate should be ("str")
    batteryReading.get.batterylevel should be (100.0)
  }

  it should "be None for BatteryReading json missing field" in {
    val tuple = Tuple2("key123", Map("date" -> "asd", "batteryState" -> "str"))

    val batteryReading = BatteryReading.from(tuple)

    batteryReading should be (None)
  }

  it should "be None for BatteryReading json being in wrong format" in {
    val tuple = Tuple2("key123", Map("date" -> "3.3.2016", "batteryState" -> 99.9, "batteryLevel" -> 100.0))

    val batteryReading = BatteryReading.from(tuple)

    batteryReading should be (None)
  }
}