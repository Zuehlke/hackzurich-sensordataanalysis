package com.zuehlke.hackzurich

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import com.zuehlke.hackzurich.common.dataformats._
import com.zuehlke.hackzurich.common.kafkautils.MessageStream
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream


/**
  * Consumes messages from one or more topics in Kafka and puts them into a cassandra table
  *
  * Run in dcos with:
  *
  * dcos spark run --submit-args="--supervise --class com.zuehlke.hackzurich.KafkaToCassandra <jar_location>"
  */
object KafkaToCassandra {

  def main(args: Array[String]) {
    val executionName = "KafkaToCassandra"

    val spark = SparkSession.builder()
      .appName(executionName)
      .config("spark.cassandra.connection.host", "node-0.cassandra.mesos,node-1.cassandra.mesos,node-2.cassandra.mesos")
      .getOrCreate()

    // Create context with 5 second batch interval
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    val messages: InputDStream[ConsumerRecord[String, String]] = MessageStream.directMessageStream(ssc, executionName)
    // More config options:, Topics.SENSOR_READING, OffsetResetConfig.Earliest)

    val keyFilter = MessageStream.filterKey

    val parsedMessages = messages
      .filter(keyFilter(_))
      .flatMap(SensorReadingJSONParser.parseReadingsUsingScalaJSONParser)

    // save Accelerometer
    val accelerometerFilter = new SensorTypeFilter("Accelerometer")
    parsedMessages
      .filter(accelerometerFilter(_))
      .map(t => AccelerometerReading(
        t._1,
        t._2.get("date").get.asInstanceOf[String],
        t._2.get("x").get.asInstanceOf[Double],
        t._2.get("y").get.asInstanceOf[Double],
        t._2.get("z").get.asInstanceOf[Double]))
      .saveToCassandra("sensordata", "accelerometer", SomeColumns("date", "deviceid", "x", "y", "z"))

    // save Battery
    val batteryFilter = new SensorTypeFilter("Battery")
    val batteryReadings = parsedMessages
      .filter(batteryFilter(_))
      .map(t => BatteryReading(
        t._1,
        t._2.get("date").get.asInstanceOf[String],
        t._2.get("batteryState").get.asInstanceOf[String],
        t._2.get("batteryLevel").get.asInstanceOf[Double]))

    batteryReadings
      .saveToCassandra("sensordata", "batteryhistory", SomeColumns("date", "deviceid", "batterystate", "batterylevel"))
    batteryReadings
      .saveToCassandra("sensordata", "batterycurrent", SomeColumns("date", "deviceid", "batterystate", "batterylevel"))

    // save Barometer
    val barometerFilter = new SensorTypeFilter("Barometer")
    parsedMessages
      .filter(barometerFilter(_))
      .map(t => BarometerReading(
        t._1,
        t._2.get("date").get.asInstanceOf[String],
        t._2.get("relativeAltitude").get.asInstanceOf[Double],
        t._2.get("pressure").get.asInstanceOf[Double]))
      .saveToCassandra("sensordata", "barometer", SomeColumns("date", "deviceid", "relativealtitude", "pressure"))

    // save Gyro
    val gyroFilter = new SensorTypeFilter("Gyro")
    parsedMessages
      .filter(gyroFilter(_))
      .map(t => GyrometerReading(
        t._1,
        t._2("date").asInstanceOf[String],
        t._2.get("x").get.asInstanceOf[Double],
        t._2.get("y").get.asInstanceOf[Double],
        t._2.get("z").get.asInstanceOf[Double]))
      .saveToCassandra("sensordata", "gyro", SomeColumns("date", "deviceid", "x", "y", "z"))

    // save Magnetometer
    val magnetoFilter = new SensorTypeFilter("Magnetometer")
    parsedMessages
      .filter(magnetoFilter(_))
      .map(t => MagnetometerReading(
        t._1,
        t._2("date").asInstanceOf[String],
        t._2.get("x").get.asInstanceOf[Double],
        t._2.get("y").get.asInstanceOf[Double],
        t._2.get("z").get.asInstanceOf[Double]))
      .saveToCassandra("sensordata", "magnetometer", SomeColumns("date", "deviceid", "x", "y", "z"))

    // save DeviceMotion
    val motionFilter = new SensorTypeFilter("DeviceMotion")
    parsedMessages
      .filter(motionFilter(_))
      .map(t => MotionReading(
        t._1,
        t._2.get("date").get.asInstanceOf[String],
        t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("quaternion").flatMap { case p3: Map[String, _] => p3.get("x")} }.get.asInstanceOf[Double],
        t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("quaternion").flatMap { case p3: Map[String, _] => p3.get("w")} }.get.asInstanceOf[Double],
        t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("quaternion").flatMap { case p3: Map[String, _] => p3.get("y")} }.get.asInstanceOf[Double],
        t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("quaternion").flatMap { case p3: Map[String, _] => p3.get("z")} }.get.asInstanceOf[Double],
        t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("rotationMatrix").flatMap { case p3: Map[String, _] => p3.get("m13")} }.get.asInstanceOf[Double],
        t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("rotationMatrix").flatMap { case p3: Map[String, _] => p3.get("m12")} }.get.asInstanceOf[Double],
        t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("rotationMatrix").flatMap { case p3: Map[String, _] => p3.get("m33")} }.get.asInstanceOf[Double],
        t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("rotationMatrix").flatMap { case p3: Map[String, _] => p3.get("m32")} }.get.asInstanceOf[Double],
        t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("rotationMatrix").flatMap { case p3: Map[String, _] => p3.get("m31")} }.get.asInstanceOf[Double],
        t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("rotationMatrix").flatMap { case p3: Map[String, _] => p3.get("m21")} }.get.asInstanceOf[Double],
        t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("rotationMatrix").flatMap { case p3: Map[String, _] => p3.get("m11")} }.get.asInstanceOf[Double],
        t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("rotationMatrix").flatMap { case p3: Map[String, _] => p3.get("m22")} }.get.asInstanceOf[Double],
        t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("rotationMatrix").flatMap { case p3: Map[String, _] => p3.get("m23")} }.get.asInstanceOf[Double],
        t._2.get("attitude").flatMap { case p2: Map[String, _] => p2.get("pitch")}.get.asInstanceOf[Double]))
      .saveToCassandra("sensordata", "motion", SomeColumns("date", "deviceid","x", "w", "y", "z", "m13", "m12", "m33", "m32", "m31", "m21", "m11", "m22", "m23", "pitch"))

    // save Microphone
    val micFilter = new SensorTypeFilter("Microphone")
    parsedMessages
      .filter(micFilter(_))
      .map(t => MicrophoneReading(
        t._1,
        t._2("date").asInstanceOf[String],
        t._2.get("peakPower").get.asInstanceOf[Double],
        t._2.get("averagePower").get.asInstanceOf[Double]))
      .saveToCassandra("sensordata", "microphone", SomeColumns("date", "deviceid", "peakpower", "averagepower"))

    // save Microphone
    val lightFilter = new SensorTypeFilter("Light")
    parsedMessages
      .filter(lightFilter(_))
      .map(t => LightReading(
        t._1,
        t._2("date").asInstanceOf[String],
        t._2.get("brightnes").get.asInstanceOf[Double]))
      .saveToCassandra("sensordata", "light", SomeColumns("date", "deviceid", "brightnes"))

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}