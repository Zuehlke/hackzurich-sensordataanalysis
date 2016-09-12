package com.zuehlke.hackzurich

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Just a very simple Spark job to test that running jobs works with builds from your environment on DC/OS.
  *
  * Run in dcos with:
  * dcos spark run --submit-args="--supervise --class com.zuehlke.hackzurich.SparkTestrun <jar_location>"
  *
  * To retrieve the output, you may navigate to the finished jobs of the Spark frameworh in open-shmack-mesos-console.sh
  * and take a look at stdout/stderr in the jobs sandbox.
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