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
  * Consumes messages from one or more topics in Kafka and stores only most recent battery readings
  *
  * Run in dcos with:
  *
  * dcos spark run --submit-args="--supervise --class com.zuehlke.hackzurich.BatteryFasttrack <jar_location>"
  */
object BatteryFasttrack {

  def main(args: Array[String]) {
    val executionName = "BatteryFasttrack"

    val spark = SparkSession.builder()
      .appName(executionName)
      .config("spark.cassandra.connection.host", "node-0.cassandra.mesos,node-1.cassandra.mesos,node-2.cassandra.mesos")
      .getOrCreate()

    // Create context with 10 second batch interval
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    val messages: InputDStream[ConsumerRecord[String, String]] = MessageStream.directMessageStream(ssc, executionName, Topics.SENSOR_READING, OffsetResetConfig.Latest)

    val keyFilter = MessageStream.filterKey

    val parsedMessages = messages
      .filter(keyFilter(_))
      .flatMap(SensorReadingJSON4SParser.parseWithJson4s)

    // save Battery
    val batteryFilter = new SensorTypeFilter("Battery")
    val batteryReadings = parsedMessages
      .filter(batteryFilter(_)).repartition(1)
      .flatMap(BarometerReadingJSON4S.from(_))

    batteryReadings
      .saveToCassandra("sensordata", "batterycurrent", SomeColumns("date", "deviceid", "batterystate", "batterylevel"))

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}