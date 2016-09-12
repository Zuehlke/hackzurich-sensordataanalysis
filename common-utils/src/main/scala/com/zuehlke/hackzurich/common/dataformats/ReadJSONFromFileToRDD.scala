package com.zuehlke.hackzurich.common.dataformats

import org.apache.spark.sql.SparkSession

// actually, just executables to run locally some parsing from files
object ReadJSONFromFileToRDD {
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

    val messages = SensorReadingJSONParser.parseUsingSparkSQL(ssql, sc.makeRDD(Array(validJson, invalidJson)))

    val gyroFilter = new SensorTypeFilter("Gyro")

    messages
      .filter(gyroFilter(_))
      .foreach(println(_))
  }
}
