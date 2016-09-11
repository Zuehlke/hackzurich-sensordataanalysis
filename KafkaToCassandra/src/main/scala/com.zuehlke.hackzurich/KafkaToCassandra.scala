package com.zuehlke.hackzurich

import java.util.Calendar

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import com.zuehlke.hackzurich.common.kafkautils.MesosKafkaBootstrapper
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import com.datastax.spark.connector.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import scala.util.parsing.json.{JSON}


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
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <keyspace> is a cassandra keyspace that contains the table to store the data
                            |  <tablename> is a cassandra table that stores the data
        """.stripMargin)
      System.exit(1)
    }

    val Array(topics, keyspace, tablename) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaToCassandra")
    sparkConf.set("spark.cassandra.connection.host", "node-0.cassandra.mesos")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val kafkaBrokers = MesosKafkaBootstrapper.mkBootstrapServersString
    println(s"Reading stream for topics ${topics} using brokers ${kafkaBrokers}")
    val topicsSet = topics.split(",").toSet

    // although we assume topics to be generic, we need to assume or make configurable the types of keys/values and corresponding deserializers
    val defaultDeserializer = "org.apache.kafka.common.serialization.StringDeserializer"

    val groupID = "KafkaToCassandra" + topics + tablename;

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBrokers,
      // ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest", // reads the oldest entries available in Kafka. Other possibility is to receive data from now on ("latest"). Default is to continue wherever a consumer with same group ID was or if it is a new group ID, start with latest.
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> defaultDeserializer,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> defaultDeserializer,
      ConsumerConfig.GROUP_ID_CONFIG -> groupID
    )

    val messages: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    // Save to Cassandra
    messages
      .filter(isGyroData)
      .map(GyroSensordata(_))
      .saveToCassandra(keyspace, tablename, SomeColumns("date", "deviceid", "x", "y", "z"))

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  def isGyroData(r: ConsumerRecord[String, String]): Boolean = {
    r.value() contains "GYROSCOPE"
  }
}
case class GyroSensordata(date: String, deviceid: String, x: Double, y: Double, z:Double)
object GyroSensordata {
  def apply(r: ConsumerRecord[String, String]): GyroSensordata = {
    def json = JSON.parseFull(r.value()).get.asInstanceOf[Map[String, Any]]
    GyroSensordata(
      json.get("date").get.asInstanceOf[String],
      r.key().toString(),
      json.get("x").get.asInstanceOf[Double],
      json.get("y").get.asInstanceOf[Double],
      json.get("z").get.asInstanceOf[Double])
  }
}
