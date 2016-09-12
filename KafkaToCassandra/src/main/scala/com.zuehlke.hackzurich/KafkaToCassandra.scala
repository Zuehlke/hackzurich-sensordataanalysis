package com.zuehlke.hackzurich

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import com.zuehlke.hackzurich.common.dataformats.{SensorReadingJSONParser, SensorTypeFilter}
import com.zuehlke.hackzurich.common.kafkautils.MessageStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._


/**
  * Consumes messages from one or more topics in Kafka and puts them into a cassandra table
  *
  * Run in dcos with:
  *
  * dcos spark run --submit-args="--supervise --conf spark.mesos.uris=http://hdfs.marathon.mesos:9000/v1/connect/hdfs-site.xml,http://hdfs.marathon.mesos:9000/v1/connect/core-site.xml --class com.zuehlke.hackzurich.KafkaToS3 <jar_location> <broker_dns:port> <topics> <tablename>
  */
object KafkaToCassandra {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: KafkaToCassandra <brokers> <topics> <tablename>
           |  <topics> is a list of one or more kafka topics to consume from
           |  <keyspace> is a cassandra keyspace that contains the table to store the data
           |  <tablename> is a cassandra table that stores the data
        """.stripMargin)
      System.exit(1)
    }

    val Array(topics, keyspace, tablename) = args

    val executionName = "KafkaToCassandra-Gyro"

    val spark = SparkSession.builder()
      .appName(executionName)
      .config("spark.cassandra.connection.host", "node-0.cassandra.mesos,node-1.cassandra.mesos,node-2.cassandra.mesos")
      .getOrCreate()

    // Create context with 5 second batch interval
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    val messages = MessageStream.directMessageStream(ssc, executionName + topics + tablename)

    val gyroFilter = new SensorTypeFilter("Gyro")
    val keyFilter = MessageStream.filterKey


    val parsedMessages = messages
      .filter(keyFilter(_))
      .flatMap(SensorReadingJSONParser.parseReadingsUsingScalaJSONParser)

    // save Gyro
    parsedMessages
      .filter(gyroFilter(_))
      .map(t => GyroSensordata(
        t._2.get("date").get.asInstanceOf[String],
        t._1,
        t._2.get("x").get.asInstanceOf[Double],
        t._2.get("y").get.asInstanceOf[Double],
        t._2.get("z").get.asInstanceOf[Double]))
     .saveToCassandra(keyspace, tablename, SomeColumns("date", "deviceid", "x", "y", "z"))

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

case class GyroSensordata(date: String, deviceid: String, x: Double, y: Double, z: Double)
