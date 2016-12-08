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

    // Create context with 30 second batch interval
    val ssc = new StreamingContext(spark.sparkContext, Seconds(30))

    val messages: InputDStream[ConsumerRecord[String, String]] = MessageStream.directMessageStream(ssc, executionName)
    // More config options:, Topics.SENSOR_READING, OffsetResetConfig.Earliest)

    val keyFilter = MessageStream.filterKey

    val parsedMessages = messages
      .filter(keyFilter(_))
      .flatMap(SensorReadingJSON4SParser.parseWithJson4s).cache()

    // spent some time to print debugging information
//    parsedMessages.foreachRDD(
//        rdd => {
//          rdd.take(1).toDebug
//            .map(firstElem => "Processing elements staring with key" + firstElem._1 + " at " + firstElem._2.get("date").getOrElse("no time"))
//                  .collectFirst(case s:String => s).
//        }
//    )

    // save Accelerometer
    val accelerometerFilter = new SensorTypeFilter("Accelerometer")
    parsedMessages
      .filter(accelerometerFilter(_))
      .flatMap(AccelerometerReadingJSON4S.from(_))
      .saveToCassandra("sensordata", "accelerometer", SomeColumns("date", "deviceid", "x", "y", "z"))

    // save Battery
    val batteryFilter = new SensorTypeFilter("Battery")
    val batteryReadings = parsedMessages
      .filter(batteryFilter(_))
      .flatMap(BatteryReadingJSON4S.from(_))

    batteryReadings
      .saveToCassandra("sensordata", "batteryhistory", SomeColumns("date", "deviceid", "batterystate", "batterylevel"))
    batteryReadings
      .saveToCassandra("sensordata", "batterycurrent", SomeColumns("date", "deviceid", "batterystate", "batterylevel"))

    // save Barometer
    val barometerFilter = new SensorTypeFilter("Barometer")
    parsedMessages
      .filter(barometerFilter(_))
      .flatMap(BarometerReadingJSON4S.from(_))
      .saveToCassandra("sensordata", "barometer", SomeColumns("date", "deviceid", "relativealtitude", "pressure"))

    // save Gyro
    val gyroFilter = new SensorTypeFilter("Gyro")
    parsedMessages
      .filter(gyroFilter(_))
      .flatMap(GyrometerReadingJSON4S.from(_))
      .saveToCassandra("sensordata", "gyro", SomeColumns("date", "deviceid", "x", "y", "z"))

    // save Magnetometer
    val magnetoFilter = new SensorTypeFilter("Magnetometer")
    parsedMessages
      .filter(magnetoFilter(_))
      .flatMap(MagnetometerReadingJSON4S.from(_))
      .saveToCassandra("sensordata", "magnetometer", SomeColumns("date", "deviceid", "x", "y", "z"))

    // save DeviceMotion
    val motionFilter = new SensorTypeFilter("DeviceMotion")
    parsedMessages
      .filter(motionFilter(_))
      .flatMap(MotionReadingJSON4S.from(_))
      .saveToCassandra("sensordata", "motion", SomeColumns("date", "deviceid","x", "w", "y", "z", "m13", "m12", "m33", "m32", "m31", "m21", "m11", "m22", "m23", "pitch"))

    // save Microphone
    val micFilter = new SensorTypeFilter("Microphone")
    parsedMessages
      .filter(micFilter(_))
      .flatMap(MicrophoneReadingJSON4S.from(_))
      .saveToCassandra("sensordata", "microphone", SomeColumns("date", "deviceid", "peakpower", "averagepower"))

    // save Microphone
    val lightFilter = new SensorTypeFilter("Light")
    parsedMessages
      .filter(lightFilter(_))
      .flatMap(LightReadingJSON4S.from(_))
      .saveToCassandra("sensordata", "light", SomeColumns("date", "deviceid", "brightnes"))

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}