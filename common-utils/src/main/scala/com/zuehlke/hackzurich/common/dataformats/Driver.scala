package com.zuehlke.hackzurich.common.dataformats

import com.zuehlke.hackzurich.common.kafkautils.MessageStream
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.{Failure, Success, Try}

object Driver {
  def main(args: Array[String]) {

     val validJson = scala.tools.nsc.io.File("/home/shmack/Downloads/Gyrometer.json").slurp()
    val invalidJson = scala.tools.nsc.io.File("/home/shmack/Downloads/Garbage.txt").slurp()

    val spark = SparkSession
      .builder()
      .appName("JSONReader")
      .master("local[*]")
      .getOrCreate()

    // Create context with 5 second batch interval
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    val messages = ssc.textFileStream("/home/shmack/Downloads/stream/")
    val sc = spark.sparkContext
    val ssql = spark.sqlContext

    val gyroFilter = new SensorTypeFilter("Gyro")

    messages
      .foreachRDD(
        MessageStream.parseJson(ssql, sc, _)
            .foreach(println(_))
          )

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }
}
