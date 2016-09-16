package com.zuehlke.hackzurich.common.dataformats

// Hint: Use only lowercase characters in case classes to avoid trouble when storing data in Cassandra,
// where rules for column names my be different

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#accelerometer */
case class AccelerometerReading(deviceid : String, date : String, x: Double, y: Double, z :  Double, sensortype: String = "Accelerometer")

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#gyrometer */
case class GyrometerReading(deviceid : String, date : String, x: Double, y: Double, z:  Double, sensortype: String = "Gyro")

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#magnetometer */
case class MagnetometerReading(deviceid : String, date : String, x: Double, y: Double, z:  Double, sensortype: String = "Magnetometer")

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#devicemotion */
case class MotionReading(deviceid : String, date : String, x: Double, w: Double, y: Double, z:  Double, m13: Double, m12: Double, m33: Double, m32: Double, m31: Double, m21: Double, m11: Double, m22: Double, m23: Double, pitch: Double, sensortype: String = "DeviceMotion")

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#barometer */
case class BarometerReading(deviceid : String, date : String, relativealtitude: Double, pressure: Double, sensortype: String = "Barometer")

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#batterylevel */
case class BatteryReading(deviceid : String, date : String, batterystate: String, batterylevel: Double, sensortype: String = "Battery")

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#microphone */
case class MicrophoneReading(deviceid : String, date : String,  peakpower: Double, averagepower: Double, sensortype: String = "Microphone")

/** See https://github.com/Zuehlke/hackzurich-sensordata-ios/blob/master/README.md#light */
case class LightReading(deviceid : String, date : String, brightnes: Double, sensortype: String = "Light")

class SensorReading {

}