package com.zuehlke.hackzurich.common.dataformats

import com.zuehlke.hackzurich.common.kafkautils.MessageStream
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.TimestampType

import scala.util.{Failure, Success, Try}

object Driver {
  def main(args: Array[String]) {

     val wholeJson = scala.tools.nsc.io.File("/home/shmack/Downloads/Gyrometer.json").slurp()
    //val wholeJson = scala.tools.nsc.io.File("/home/shmack/Downloads/Garbage.txt").slurp()

    val spark = SparkSession
      .builder()
      .appName("JSONReader")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val ssql = spark.sqlContext

    val df = MessageStream.parseJson(ssql, sc, wholeJson)
    df.printSchema()

    println(df.count())
    df
      .filter(record => SensorData.isOfSensorType(record, "Gyro"))
      .rdd.foreach( println )
  }
}
