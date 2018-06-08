package com.zuehlke.hackzurich

import java.util.Properties

import com.zuehlke.hackzurich.common.kafkautils.MesosKafkaBootstrapper
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SetupUtils {
  def createSparkSession(executionName: String): SparkSession = {
    val cassandraNodes = "node-0-server.cassandra.autoip.dcos.thisdcos.directory,node-1-server.cassandra.autoip.dcos.thisdcos.directory,node-2-server.cassandra.autoip.dcos.thisdcos.directory"
    val sparkConf = new SparkConf(true).set("spark.cassandra.connection.host", cassandraNodes)
    val spark = SparkSession.builder()
      .appName(executionName)
      .config(sparkConf)
      .getOrCreate()
    spark
  }

  def createKafkaProducerProperties(): Properties = {
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", MesosKafkaBootstrapper.mkBootstrapServersString)
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps
  }
}
