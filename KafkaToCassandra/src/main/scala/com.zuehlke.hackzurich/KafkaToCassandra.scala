package com.zuehlke.hackzurich

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import com.datastax.spark.connector.streaming._
import com.zuehlke.hackzurich.common.kafkautils.MessageStream
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Consumes messages from one or more topics in Kafka and puts them into a cassandra table
  * So far, most stuff hard-wired with the default / corresponding names.
  */
object KafkaToCassandra {
  def main(args: Array[String]) {
    val executionName = "KafkaToCassandra-Gyro"
    // Create context with 5 second batch interval

    val spark = SparkSession.builder().appName(executionName).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val ssql = spark.sqlContext

    val messages = MessageStream.directMessageStream(ssc, executionName)

    // Need to transform data from JSON to com.zuehlke.hackzurich.common.dataformats.GyrometerReading
    // Would like to do that like this: ... but that would build an RDD of RDD. Not good.
     //messages.flatMap(record => Tuple2(record.key, MessageStream.parseJson(ssql, spark.sparkContext, record.value())

    // Save to Cassandra
    messages.saveToCassandra("sensordata", "sensorreading_by_device_sensortype", SomeColumns("key", "data"))

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
