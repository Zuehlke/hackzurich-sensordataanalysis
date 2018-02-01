package com.zuehlke.hackzurich

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time._
import java.util.Properties

import com.cloudera.sparkts.models.ARIMA
import com.cloudera.sparkts.{DateTimeIndex, SecondFrequency, TimeSeriesRDD}
import com.zuehlke.hackzurich.common.kafkautils.MesosKafkaBootstrapper
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._


/**
  * Reads data from Cassandra and performs data analytics with Spark MLlib
  * The
  *
  * Run in dcos with:
  *
  * dcos spark run --submit-args="--class com.zuehlke.hackzurich.DataAnalytics <jar_location>"
  */
object DataAnalytics {

  /**
    * Initial value of latest timestamp
    */
  var latestDate = 0l

  /**
    * How far in the future we want to predict
    */
  val forecastTime = 5


  /**
    * Updates the latest timestmap
    *
    * @param date timestamp to store if later than `currentDate`
    */
  def updateLatestDate(date: Long): Unit = {
    latestDate = if (date > latestDate) date else latestDate
  }

  /**
    * UserDefinedFunction to create a new Timestamp from given input columns
    */
  val toDateUdf: UserDefinedFunction = udf((year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int) => {
    new Timestamp(year - 1900, month, day, hour, minute, second, 0)
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

    // Read data from Cassandra
    val barometerDf = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "keyspace" -> "sensordata",
        "table" -> "barometer"))
      .load()

    // Aggregate data to a precision of seconds and calculate mean
    var observations = barometerDf.select("date", "deviceid", "relativealtitude")
    observations = observations.groupBy(
      year(col("date")).alias("year"),
      month(col("date")).alias("month"),
      dayofmonth(col("date")).alias("day"),
      hour(col("date")).alias("hour"),
      minute(col("date")).alias("minute"),
      second(col("date")).alias("second"),
      col("deviceid")
    ).agg(
      count(col("deviceid")).as("number_of_devices"),
      mean("relativealtitude").as("relativealtitude_average"))

    // Add converted (aggregated) date back again
    observations = observations.withColumn("timestamp", toDateUdf.apply(col("year"), col("month"), col("day"), col("hour"), col("minute"), col("second")))
    observations.show(10, false)

    //////////////////////////////////////////////////////////////////////////
    //////////////////////////// PREDICTION PART /////////////////////////////
    //////////////////////////////////////////////////////////////////////////

    val firstDate: Timestamp = observations.select("timestamp").orderBy(asc("timestamp")).head().getAs[Timestamp]("timestamp")
    val lastDate: Timestamp = observations.select("timestamp").orderBy(desc("timestamp")).head().getAs[Timestamp]("timestamp")

    // Create a DateTimeIndex over the whole range of input data in seconds interval
    val zone = ZoneId.systemDefault()
    val dtIndex = DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(firstDate.toLocalDateTime, zone),
      ZonedDateTime.of(lastDate.toLocalDateTime, zone),
      new SecondFrequency(1))

    // Align the data on the DateTimeIndex to create a TimeSeriesRDD
    val timeSeriesrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, observations,
      "timestamp", "deviceid", "relativealtitude_average")

    // Compute missing values using linear interpolation
    val interpolatedTimeSeriesrdd = timeSeriesrdd.fill("linear")

    val forecast = interpolatedTimeSeriesrdd.mapSeries { vector =>
      /*
      * If we are missing some data in the beginning or at the end (so everything we could not interpolate)
      * we just give the prediction based on the latest data we have (which is everything != NaN)
      */
      val newVec = new DenseVector(vector.toArray.filter(!_.isNaN))

      val arimaModel = ARIMA.fitModel(1, 1, 0, newVec)
      val forecasted = arimaModel.forecast(newVec, forecastTime)
      new DenseVector(forecasted.toArray.slice(forecasted.size - forecastTime, forecasted.size))
    }

    forecast.foreach { x =>
      val timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(System.currentTimeMillis())
      val message = s"$timestamp forecast size: ${x._2.size} , $x , ${x._2}"
      new KafkaProducer[String, String](producerProps).send(new ProducerRecord("debug", System.currentTimeMillis().toString, message))
    }

    //////////////////////////////////////////////////////////////////////////
    //////////////////////////// PREDICTION PART /////////////////////////////
    //////////////////////////////////////////////////////////////////////////

    while (true) {
      barometerDf
        .where(s"date > '$latestDate'")
        .foreach { x =>
          val producer = new KafkaProducer[String, String](producerProps)
          val date = x.getAs[Timestamp]("date")
          val dateFormatted = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(date)
          val deviceid = x.getAs[String]("deviceid")
          val relativealtitude = x.getAs[Double]("relativealtitude")
          val pressure = x.getAs[Double]("pressure")
          val message = s"$dateFormatted - $deviceid - $relativealtitude - $pressure"
          println(message)

          val record = new ProducerRecord("data-analytics", "barometer", message)
          producer.send(record)
          updateLatestDate(date.getTime)
        }

      Thread.sleep(15 * 1000)
    }

    sc.stop()
  }
}