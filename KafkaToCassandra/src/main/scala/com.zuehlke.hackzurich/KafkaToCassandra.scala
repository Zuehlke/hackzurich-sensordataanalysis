package com.zuehlke.hackzurich

import com.datastax.spark.connector.SomeColumns
import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf}
import com.datastax.spark.connector.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils

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
      System.err.println(s"""
                            |Usage: KafkaToCassandra <brokers> <topics> <tablename>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <tablename> is a cassandra table that stores the data
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics, tablename) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaToS3")
    val ssc = new StreamingContext(sparkConf, Seconds(30))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)


    // Save to Cassandra
    messages.saveToCassandra("hackzurichzuhlke", tablename, SomeColumns("key", "data"))

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
