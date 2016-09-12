package com.zuehlke.hackzurich.common.dataformats

// Hint: Use only lowercase characters in case classes to avoid trouble when storing data in Cassandra,
// where rules for column names my be different
case class GyrometerReading(deviceid : String, date : String, x: Double, y: Double, z :  Double, sensortype: String = "Gyro")
case class BatteryReading(deviceid : String, date : String, batterystate: String, batterylevel: Double, sensortype: String = "Battery")

class SensorReading {

}
