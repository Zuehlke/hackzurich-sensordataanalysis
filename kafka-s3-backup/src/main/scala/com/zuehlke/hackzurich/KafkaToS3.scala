package com.zuehlke.hackzurich

import java.util.Calendar

import com.zuehlke.hackzurich.common.kafkautils.MesosKafkaBootstrapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * Consumes messages from one or more topics in Kafka and puts them into an S3 bucket.
  *
  * Run in dcos with:
  * dcos spark run --submit-args="--supervise --conf spark.mesos.uris=http://hdfs.marathon.mesos:9000/v1/connect/hdfs-site.xml,http://hdfs.marathon.mesos:9000/v1/connect/core-site.xml --class com.zuehlke.hackzurich.KafkaToS3 <jar_location> <topics> <s3bucket> <awsId> <awsSecret>"
  */
object KafkaToS3 {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(s"""
                            |Usage: KafkaToS3 <topics> <s3bucket> <awsAccessKeyId> <awsAccesKeySecret>
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <s3bucket> the s3bucket to store the files into (s3bucket may contain subfolders, will get created if needed)
                            |  <awsAccessKeyId> the aws access key
                            |  <awsAccesKeySecret> the aws access secret
        """.stripMargin)
      System.exit(1)
    }

    val Array(topics, s3bucket, awsId, awsSecret) = args

    // Create context with 30 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaToS3")
    val ssc = new StreamingContext(sparkConf, Seconds(30))

    // use Hadoop HDFS tooling to access S3
    ssc.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId",awsId)
    ssc.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey",awsSecret)

    // Create direct kafka stream with brokers and topics
    val kafkaBrokers = MesosKafkaBootstrapper.mkBootstrapServersString
    println(s"Reading stream for topics ${topics} using brokers ${kafkaBrokers}")
    val topicsSet = topics.split(",").toSet

    // although we assume topics to be generic, we need to assume or make configurable the types of keys/values and corresponding deserializers
    val defaultDeserializer = "org.apache.kafka.common.serialization.StringDeserializer"

    val groupID = "KafkaToS3" + topics + s3bucket;

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBrokers,
      // ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest", // reads the oldest entries available in Kafka. Other possibility is to receive data from now on ("latest"). Default is to continue wherever a consumer with same group ID was or if it is a new group ID, start with latest.
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> defaultDeserializer,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> defaultDeserializer,
      ConsumerConfig.GROUP_ID_CONFIG -> groupID
    )

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    // Save to S3
    messages.foreachRDD(rdd => {
      println("\n\nNumber of records in this batch : " + rdd.count())
        if(rdd.count() > 0){
          rdd.flatMap(record => s"${record.key} : ${record.value()}")
          .repartition(1).saveAsTextFile("s3n://" + s3bucket + "/" + Calendar.getInstance().getTime())
        }
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}