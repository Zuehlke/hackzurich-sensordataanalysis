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

    val spark = SparkSession.builder()
      .appName(executionName)
      .config("spark.cassandra.connection.host","node-0.cassandra.mesos,node-1.cassandra.mesos,node-2.cassandra.mesos")
      .getOrCreate()

    // Create context with 5 second batch interval
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    val messages = MessageStream.directMessageStream(ssc, executionName)

    val ssql = spark.sqlContext

    messages
      .filter(r => r.key.nonEmpty && !r.key.equalsIgnoreCase("(none)") && r.key.length > 0)
      .map(r => Tuple2(r.key, ssql.read.json(spark.sparkContext.makeRDD(Array(r.value())))))




    // Save to Cassandra
    //messages.saveToCassandra("sensordata", "sensorreading_by_device_sensortype", SomeColumns("key", "data"))

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
