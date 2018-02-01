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
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


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
    * The global DataFrame which is holding the observations aggregated from the Cassandra DB
    * This variable is updated after polling by unioning the new data
    */
  var observationsDf: DataFrame = null

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

    /*
     * We do this whole stuff in a loop because we want to poll Cassandra regularly
     */
    while (true) {

      // Read data from Cassandra
      var barometerDf = spark
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map(
          "keyspace" -> "sensordata",
          "table" -> "barometer"))
        .load()

      // If we've never loaded any data from Cassandra we don't want to filter out anything
      if (observationsDf != null) {
        // This filter is pushed down to Cassandra and only serialized the data we need!
        barometerDf = barometerDf.filter(s"date > '$latestDate'")
      }

      // Aggregate data to a precision of seconds and calculate the mean
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

      if (observationsDf == null) {
        observationsDf = observations
      } else {
        observationsDf = observationsDf.union(observations).distinct()
      }

      println(s"observationsDf.count() = ${observationsDf.count()}")
      observationsDf.show(10, false)

      //////////////////////////////////////////////////////////////////////////
      //////////////////////////// PREDICTION PART /////////////////////////////
      //////////////////////////////////////////////////////////////////////////

      val firstDate: Timestamp = observationsDf.select("timestamp").orderBy(asc("timestamp")).head().getAs[Timestamp]("timestamp")
      val lastDate: Timestamp = observationsDf.select("timestamp").orderBy(desc("timestamp")).head().getAs[Timestamp]("timestamp")

      println(s"firstDate: $firstDate , lastDate: $lastDate")

      // Create a DateTimeIndex over the whole range of input data in seconds interval
      val zone = ZoneId.systemDefault()
      val dtIndex = DateTimeIndex.uniformFromInterval(
        ZonedDateTime.of(firstDate.toLocalDateTime, zone),
        ZonedDateTime.of(lastDate.toLocalDateTime, zone),
        new SecondFrequency(1))

      // Align the data on the DateTimeIndex to create a TimeSeriesRDD
      val timeSeriesrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, observationsDf,
        "timestamp", "deviceid", "relativealtitude_average")

      // Compute missing values using linear interpolation
      val interpolatedTimeSeriesrdd = timeSeriesrdd.fill("linear")

      val forecast = interpolatedTimeSeriesrdd.mapSeries { vector =>
        /*
        * If we are missing some data in the beginning or at the end (so everything we could not interpolate)
        * we just give the prediction based on the latest data we have (which is everything != NaN)
        */
        val newVec = new DenseVector(vector.toArray.filter(!_.isNaN))
        require(newVec.size > 0)

        val arimaModel = ARIMA.fitModel(1, 1, 0, newVec)
        val forecasted = arimaModel.forecast(newVec, forecastTime)
        new DenseVector(forecasted.toArray.slice(forecasted.size - forecastTime, forecasted.size))
      }

      forecast.foreach { x =>
        val timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(System.currentTimeMillis())
        val message = s"$timestamp forecast size: ${x._2.size} , $x}"
        new KafkaProducer[String, String](producerProps).send(new ProducerRecord("data-analytics", x._1, message))
      }

      //////////////////////////////////////////////////////////////////////////
      //////////////////////////// PREDICTION PART /////////////////////////////
      //////////////////////////////////////////////////////////////////////////

      updateLatestDate(lastDate.getTime)
    }

    sc.stop()
  }

}