package com.zuehlke.hackzurich

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession


/**
  * Reads data from Cassandra and performs data analytics in form of predictions
  * with Spark MLlib and cloudera.sparkts (time series library)
  */
object DataAnalytics {

  /**
    * The concrete implementation which will perform the prediction itself
    */
  val prediction: IPrediction = PredictionImpl


  def main(args: Array[String]) {
    val spark: SparkSession = SetupUtils.createSparkSession("SparkDataAnalytics")
    val sc = spark.sparkContext
    val producerProperties = SetupUtils.createKafkaProducerProperties()

    val timeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var iteration = 1

    /*
     * We perform this in a loop, because we want to poll Cassandra regularly
     */
    while (true) {
      println(timeFormatter.format(System.currentTimeMillis()) + s" Starting iteration $iteration")
      iteration += 1

      // Read data from Cassandra
      val dataFrame = CassandraLoader.loadDataFrame(spark)

      // Perform the prediction
      prediction.performPrediction(dataFrame, producerProperties)
    }

    sc.stop()
  }

}