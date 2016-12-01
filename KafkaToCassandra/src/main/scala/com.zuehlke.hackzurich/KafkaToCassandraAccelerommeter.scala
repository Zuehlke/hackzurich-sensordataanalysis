package com.zuehlke.hackzurich

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import com.zuehlke.hackzurich.common.dataformats._
import com.zuehlke.hackzurich.common.kafkautils.MessageStream.OffsetResetConfig
import com.zuehlke.hackzurich.common.kafkautils.{MessageStream, Topics}
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
object KafkaToCassandraAccelerommeter {

  def main(args: Array[String]) {
    val executionName = "KafkaToCassandraAccelerommeter"

    val spark = SparkSession.builder()
      .appName(executionName)
      .config("spark.cassandra.connection.host", "node-0.cassandra.mesos,node-1.cassandra.mesos,node-2.cassandra.mesos, node-3.cassandra.mesos, node-4.cassandra.mesos")
      .getOrCreate()

    // Create context with 30 second batch interval
    val ssc = new StreamingContext(spark.sparkContext, Seconds(30))

    val messages: InputDStream[ConsumerRecord[String, String]] = MessageStream.directMessageStream(ssc, executionName, Topics.SENSOR_READING, OffsetResetConfig.Earliest)
    // More config options:, Topics.SENSOR_READING, OffsetResetConfig.Earliest)

    val keyFilter = MessageStream.filterKey
    val accelerometerFilter = new SensorTypeFilterJSON4S("Accelerometer")

    val parsedMessages = messages
      .filter(keyFilter(_))
      .flatMap(SensorReadingJSON4SParser.parseWithJson4s)
      .filter(accelerometerFilter(_))
      .flatMap(AccelerometerReadingJSON4S.from(_))
      .saveToCassandra("sensordata", "accelerometer", SomeColumns("date", "deviceid", "x", "y", "z"))

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}