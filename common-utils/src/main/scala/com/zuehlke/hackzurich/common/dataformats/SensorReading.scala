package com.zuehlke.hackzurich.common.dataformats

// Hint: Use only lowercase characters in case classes to avoid trouble when storing data in Cassandra,
// where rules for column names my be different
case class AccelerometerReading(deviceid : String, date : String, x: Double, y: Double, z :  Double, sensortype: String = "Accelerometer")
case class BatteryReading(deviceid : String, date : String, batterystate: String, batterylevel: Double, sensortype: String = "Battery")
case class BarometerReading(deviceid : String, date : String, relativealtitude: Double, pressure: Double, sensortype: String = "Barometer")
case class GyrometerReading(deviceid : String, date : String, x: Double, y: Double, z:  Double, sensortype: String = "Gyro")
case class MotionReading(deviceid : String, date : String, x: Double, w: Double, y: Double, z:  Double, m13: Double, m12: Double, m33: Double, m32: Double, m31: Double, m21: Double, m11: Double, m22: Double, m23: Double, pitch: Double, sensortype: String = "DeviceMotion")

class SensorReading {

}