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
  * Consumes messages from one or more topics in Kafka and puts them into a cassandra table + publish to Kafka
  *
  * Run in dcos with:
  *
  * dcos spark run --submit-args="--supervise --class com.zuehlke.hackzurich.KafkaToAccelerometer <jar_location>"
  */
object KafkaToAccelerometer {

  val KAFKA_PUBLISH_SIZE = 100

  def main(args: Array[String]) {
    val executionName = "KafkaToAccelerometer"

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

    // Create context with 40 second batch interval
    val ssc = new StreamingContext(spark.sparkContext, Seconds(40))

    val messages: InputDStream[ConsumerRecord[String, String]] = MessageStream.directMessageStream(ssc, executionName)

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
      rdd.take(KAFKA_PUBLISH_SIZE).foreach { accelerometer =>
        new KafkaProducer[String, String](producerProps).send(new ProducerRecord(Topics.SENSOR_READING_ACCELEROMETER, accelerometer.deviceid, accelerometer.toCsv))
      }
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}