package com.zuehlke.hackzurich

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import com.datastax.spark.connector._
import com.zuehlke.hackzurich.common.kafkautils.MesosKafkaBootstrapper
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  * Reads data from Cassandra and performs data analytics with Spark MLlib
  * The
  *
  * Run in dcos with:
  *
  * dcos spark run --submit-args="--supervise --class com.zuehlke.hackzurich.DataAnalytics <jar_location>"
  */
object DataAnalytics {

  var latestDate = 0l

  def updateLatestDate(date: Long): Unit = {
    latestDate = if (date > latestDate) date else latestDate
  }

  def main(args: Array[String]) {
    val executionName = "SparkDataAnalytics"

    val sparkConf = new SparkConf(true).set("spark.cassandra.connection.host", "node-0-server.cassandra.autoip.dcos.thisdcos.directory,node-1-server.cassandra.autoip.dcos.thisdcos.directory,node-2-server.cassandra.autoip.dcos.thisdcos.directory")
    val spark = SparkSession.builder()
      .appName(executionName)
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext

    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", MesosKafkaBootstrapper.mkBootstrapServersString)
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val barometer = sc.cassandraTable("sensordata", "barometer")

    while (true) {
      barometer
        .where(s"date > '$latestDate'")
        .foreach { x =>
          val producer = new KafkaProducer[String, String](producerProps)
          val date = x.getLong("date")
          new Timestamp(date)
          val dateFormatted = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Timestamp(date))
          val deviceid = x.getString("deviceid")
          val relativealtitude = x.getDouble("relativealtitude")
          val pressure = x.getDouble("pressure")
          val message = s"$dateFormatted - $deviceid - $relativealtitude - $pressure"
          println(message)

          val record = new ProducerRecord("data-analytics", System.currentTimeMillis().toString, message)
          producer.send(record)
          updateLatestDate(date)
        }

      Thread.sleep(15 * 1000)
    }

    sc.stop()
  }
}