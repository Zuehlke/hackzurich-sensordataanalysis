package com.zuehlke.hackzurich

import org.apache.spark.sql.{DataFrame, SparkSession}

object CassandraLoader {

  def loadDataFrame(spark: SparkSession, deviceId: String = null): DataFrame = {
    val dataFrame = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "keyspace" -> "sensordata",
        "table" -> "accelerometer"))
      .load()

    deviceId match {
      case null => dataFrame
      case device => dataFrame.where(s"deviceid = '$device'")
    }
  }

}
