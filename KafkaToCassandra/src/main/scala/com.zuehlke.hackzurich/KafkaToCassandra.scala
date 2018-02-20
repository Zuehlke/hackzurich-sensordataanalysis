package com.zuehlke.hackzurich

import java.util.Properties

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._
import com.zuehlke.hackzurich.common.dataformats._
import com.zuehlke.hackzurich.common.kafkautils.{MesosKafkaBootstrapper, MessageStream, Topics}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
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

    val sparkConf = new SparkConf(true).set("spark.cassandra.connection.host", "node-0-server.cassandra.autoip.dcos.thisdcos.directory,node-1-server.cassandra.autoip.dcos.thisdcos.directory,node-2-server.cassandra.autoip.dcos.thisdcos.directory")
    val spark = SparkSession.builder()
      .appName(executionName)
      .config(sparkConf)
      .getOrCreate()


    val connector = CassandraConnector(sparkConf)
    try {
      val session = connector.openSession()
      CassandraCQL.createSchema(session)
    }
    catch {
      case e: Exception => System.err.println("Could not create Schema: " + e)
    }

    // Kafka Properties
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", MesosKafkaBootstrapper.mkBootstrapServersString)
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Create context with 30 second batch interval
    val ssc = new StreamingContext(spark.sparkContext, Seconds(30))

    val messages: InputDStream[ConsumerRecord[String, String]] = MessageStream.directMessageStream(ssc, executionName)
    // More config options:, Topics.SENSOR_READING, OffsetResetConfig.Earliest)

    val keyFilter = MessageStream.filterKey()

    val parsedMessages = messages
      .filter(keyFilter(_))
      .flatMap(SensorReadingJSON4SParser.parseWithJson4s).cache()

    // save Accelerometer
    val accelerometerFilter = new SensorTypeFilter("Accelerometer")
    val parsedAccelerometerMessages = parsedMessages
      .filter(accelerometerFilter(_))
      .flatMap(AccelerometerReadingJSON4S.from)
    parsedAccelerometerMessages
      .saveToCassandra("sensordata", "accelerometer", SomeColumns("date", "deviceid", "x", "y", "z"))

    // publish latest accelerometer data to Kafka for the data-analytics application
    parsedAccelerometerMessages.foreachRDD { rdd =>
      // don't publish everything, just take the newest 10 elements (we can still adjust this value later if needed)
      rdd.top(10).foreach { accelerometer =>
        new KafkaProducer[String, String](producerProps).send(new ProducerRecord(Topics.SENSOR_READING_ACCELEROMETER, accelerometer.deviceid, accelerometer.toCsv))
      }
    }

    // save Battery
    val batteryFilter = new SensorTypeFilter("Battery")
    val batteryReadings = parsedMessages
      .filter(batteryFilter(_))
      .flatMap(BatteryReadingJSON4S.from)

    batteryReadings
      .saveToCassandra("sensordata", "batteryhistory", SomeColumns("date", "deviceid", "batterystate", "batterylevel"))
    batteryReadings
      .saveToCassandra("sensordata", "batterycurrent", SomeColumns("date", "deviceid", "batterystate", "batterylevel"))

    // save Barometer
    val barometerFilter = new SensorTypeFilter("Barometer")
    parsedMessages
      .filter(barometerFilter(_))
      .flatMap(BarometerReadingJSON4S.from)
      .saveToCassandra("sensordata", "barometer", SomeColumns("date", "deviceid", "relativealtitude", "pressure"))

    // save Gyro
    val gyroFilter = new SensorTypeFilter("Gyro")
    parsedMessages
      .filter(gyroFilter(_))
      .flatMap(GyrometerReadingJSON4S.from)
      .saveToCassandra("sensordata", "gyro", SomeColumns("date", "deviceid", "x", "y", "z"))

    // save Magnetometer
    val magnetoFilter = new SensorTypeFilter("Magnetometer")
    parsedMessages
      .filter(magnetoFilter(_))
      .flatMap(MagnetometerReadingJSON4S.from)
      .saveToCassandra("sensordata", "magnetometer", SomeColumns("date", "deviceid", "x", "y", "z"))

    // save DeviceMotion
    val motionFilter = new SensorTypeFilter("DeviceMotion")
    parsedMessages
      .filter(motionFilter(_))
      .flatMap(MotionReadingJSON4S.from)
      .saveToCassandra("sensordata", "motion", SomeColumns("date", "deviceid", "x", "w", "y", "z", "m13", "m12", "m33", "m32", "m31", "m21", "m11", "m22", "m23", "pitch"))

    // save Microphone
    val micFilter = new SensorTypeFilter("Microphone")
    parsedMessages
      .filter(micFilter(_))
      .flatMap(MicrophoneReadingJSON4S.from)
      .saveToCassandra("sensordata", "microphone", SomeColumns("date", "deviceid", "peakpower", "averagepower"))

    // save Microphone
    val lightFilter = new SensorTypeFilter("Light")
    parsedMessages
      .filter(lightFilter(_))
      .flatMap(LightReadingJSON4S.from)
      .saveToCassandra("sensordata", "light", SomeColumns("date", "deviceid", "brightnes"))

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}