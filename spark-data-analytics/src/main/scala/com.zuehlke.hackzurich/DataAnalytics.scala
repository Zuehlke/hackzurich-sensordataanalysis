package com.zuehlke.hackzurich

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time._
import java.util.Properties

import com.cloudera.sparkts.models.ARIMA
import com.cloudera.sparkts.{DateTimeIndex, SecondFrequency, TimeSeriesRDD}
import com.zuehlke.hackzurich.common.dataformats.Prediction
import com.zuehlke.hackzurich.common.kafkautils.{MesosKafkaBootstrapper, Topics}
import org.apache.commons.math3.exception.{MathIllegalArgumentException, NoDataException}
import org.apache.commons.math3.linear.SingularMatrixException
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

    /*
     * We do this whole stuff in a loop because we want to poll Cassandra regularly
     */
    while (true) {
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
        "timestamp", "deviceid", "z_average")

      // Compute missing values using linear interpolation
      val interpolatedTimeSeriesrdd = timeSeriesrdd.fill("linear")

      val forecast = interpolatedTimeSeriesrdd.mapSeries { vector =>
        /*
        * If we are missing some data in the beginning or at the end (so everything we could not interpolate)
        * we just give the prediction based on the latest data we have (which is everything != NaN)
        */
        val doubles = vector.toArray.filter(!_.isNaN)
        val newVec = if (doubles.length > 0)
          new DenseVector(doubles)
        else
          new DenseVector(Array(0.0))

        var returnValue = newVec

        try {
          val arimaModel = ARIMA.fitModel(1, 1, 0, newVec)
          val forecasted = arimaModel.forecast(newVec, forecastTime)
          returnValue = new DenseVector(forecasted.toArray.slice(forecasted.size - forecastTime, forecasted.size))
        } catch {
          case e: NegativeArraySizeException => println(s"Error with array: $newVec - ${e.getMessage}")
            returnValue = new DenseVector(Array(0.0))
          case e: SingularMatrixException => println(s"The result matrix of the model was singular: ${e.getMessage}")
            returnValue = new DenseVector(Array(0.0))
          case e: NoDataException => println(s"There was no input data: ${e.getMessage}")
            returnValue = new DenseVector(Array(0.0))
          case e: MathIllegalArgumentException => println(s"Math exception: ${e.getMessage}")
            returnValue = new DenseVector(Array(0.0))
        }
        returnValue
      }

      forecast.foreach { x =>
        val predictionMessage = Prediction(lastDate.getTime, x._1, x._2.toArray)
        new KafkaProducer[String, String](producerProps).send(new ProducerRecord(Topics.DATA_ANALYTICS, x._1, predictionMessage.toCsv))
      }
    }

    sc.stop()
  }

}