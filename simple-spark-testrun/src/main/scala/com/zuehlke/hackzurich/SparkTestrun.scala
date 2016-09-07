package com.zuehlke.hackzurich

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Consumes messages from one or more topics in Kafka and puts them into an S3 bucket.
  *
  * Run in dcos with:
  * dcos spark run --submit-args="--supervise --class com.zuehlke.hackzurich.SparkTestrun <jar_location>"
  */
object SparkTestrun {
  def main(args: Array[String]) {
    println(s"Scala version: ${scala.util.Properties.versionString}")

    val sparkConf = new SparkConf().setAppName("SparkTestrun")
    val sc = new SparkContext(sparkConf)
    println(s"Spark version: ${sc.version}")

    val rdd = sc.parallelize(1 to 5)
    println(rdd.sum())
  }
}