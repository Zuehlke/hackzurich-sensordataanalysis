package com.zuehlke.hackzurich.common.dataformats

import com.zuehlke.hackzurich.common.kafkautils.MessageStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.{Failure, Success, Try}
import scala.util.parsing.json.JSON

// actually, just executables to run locally some parsing from files
object ReadJSONFromFileMap {
  def main(args: Array[String]) {

     val validJson = scala.tools.nsc.io.File("/home/shmack/Downloads/Gyrometer.json").slurp()
    val invalidJson = scala.tools.nsc.io.File("/home/shmack/Downloads/Garbage.txt").slurp()

    val spark = SparkSession
      .builder()
      .appName("JSONReader")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val ssql = spark.sqlContext


    val rdd = sc.makeRDD(Array(validJson, invalidJson))
    val gyroFilter = new SensorTypeFilter("Gyro")

    rdd
      .flatMap(com.zuehlke.hackzurich.common.dataformats.SensorReadingJSONParser.parseReadingsUsingScalaJSONParser)
      .filter(gyroFilter(_))
      .foreach(println)

  }
}
