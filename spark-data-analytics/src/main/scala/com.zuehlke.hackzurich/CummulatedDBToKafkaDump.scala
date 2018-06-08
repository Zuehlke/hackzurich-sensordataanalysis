package com.zuehlke.hackzurich

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import com.zuehlke.hackzurich.common.kafkautils.MesosKafkaBootstrapper
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/**
  * Reads data from Cassandra and writes the cummulated data to a Kafka topic
  */
object CummulatedDBToKafkaDump {

  /**
    * Iteration counter
    */
  var iteration = 1

  /**
    * How far in the future we want to predict
    */
  val forecastTime = 30

  /**
    * Time Formatter for debug output
    */
  val timeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  /**
    * UserDefinedFunction to create a new Timestamp from given input columns
    */
  val toDateUdf: UserDefinedFunction = udf((year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int) => {
    val timeFormatted = s"$year-$month-$day $hour:$minute:$second.0"
    Timestamp.valueOf(timeFormatted)
  })

  def main(args: Array[String]) {
    val executionName = "SparkDataAnalytics"

    val cassandraNodes = "node-0-server.cassandra.autoip.dcos.thisdcos.directory,node-1-server.cassandra.autoip.dcos.thisdcos.directory,node-2-server.cassandra.autoip.dcos.thisdcos.directory"
    val sparkConf = new SparkConf(true).set("spark.cassandra.connection.host", cassandraNodes)
    val spark = SparkSession.builder()
      .appName(executionName)
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext

    // Kafka Properties
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", MesosKafkaBootstrapper.mkBootstrapServersString)
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    println(timeFormatter.format(System.currentTimeMillis()) + s" Starting iteration $iteration")
    iteration += 1

    // Read data from Cassandra
    var barometerDf = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "keyspace" -> "sensordata",
        "table" -> "accelerometer"))
      .load()

    // Aggregate data to a precision of seconds and calculate the mean
    var observations = barometerDf.select("date", "deviceid", "z")
    observations = observations.groupBy(
      year(col("date")).alias("year"),
      month(col("date")).alias("month"),
      dayofmonth(col("date")).alias("day"),
      hour(col("date")).alias("hour"),
      minute(col("date")).alias("minute"),
      second(col("date")).alias("second"),
      col("deviceid")
    ).agg(mean("z").as("z_average"))

    // Add converted (aggregated) date back again
    observations = observations.withColumn("timestamp", toDateUdf.apply(col("year"), col("month"), col("day"), col("hour"), col("minute"), col("second")))

    new KafkaProducer[String, String](producerProps).send(new ProducerRecord("dump", "", "timestamp ; year ; month ; day ; hour ; minute ; second ; deviceid ; z_average"))
    observations.rdd.foreach { r =>
      val year = r.getAs[Int]("year")
      val month = r.getAs[Int]("month")
      val day = r.getAs[Int]("day")
      val hour = r.getAs[Int]("hour")
      val minute = r.getAs[Int]("minute")
      val second = r.getAs[Int]("second")
      val deviceid = r.getAs[String]("deviceid")
      val z_average = r.getAs[Double]("z_average")

      val timeFormatted = s"$year-$month-$day $hour:$minute:$second.0"
      val timestamp = Timestamp.valueOf(timeFormatted).getTime

      val csv = s"$timestamp ; $year ; $month ; $day ; $hour ; $minute ; $second ; $deviceid ; $z_average"
      new KafkaProducer[String, String](producerProps).send(new ProducerRecord("dump", deviceid, csv))
    }

    sc.stop()
  }

}