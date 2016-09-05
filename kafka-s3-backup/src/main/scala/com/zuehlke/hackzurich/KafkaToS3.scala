package com.zuehlke.hackzurich

import java.util.Calendar

import com.zuehlke.hackzurich.common.kafkautils.MesosKafkaBootstrapper
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
  * Consumes messages from one or more topics in Kafka and puts them into an S3 bucket.
  *
  * Run in dcos with:
  * dcos spark run --submit-args="--supervise --conf spark.mesos.uris=http://hdfs.marathon.mesos:9000/v1/connect/hdfs-site.xml,http://hdfs.marathon.mesos:9000/v1/connect/core-site.xml --class com.zuehlke.hackzurich.KafkaToS3 <jar_location> <topics> <awsId> <awsSecret>
  */
object KafkaToS3 {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println(s"""
                            |Usage: KafkaToS3 <brokers> <topics> <s3bucket> <awsAccessKeyId> <awsAccesKeySecret>
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <s3bucket> the s3bucket to store the files into
                            |  <awsAccessKeyId> the aws access key
                            |  <awsAccesKeySecret> the aws access secret
        """.stripMargin)
      System.exit(1)
    }

    val Array(topics, s3bucket, awsId, awsSecret) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaToS3")
    val ssc = new StreamingContext(sparkConf, Seconds(30))

    ssc.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId",awsId)
    ssc.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey",awsSecret)

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> MesosKafkaBootstrapper.mkBootstrapServersString)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Save to S3
    messages.foreachRDD(rdd => {

      println("\n\nNumber of records in this batch : " + rdd.count())
      if(rdd.count() > 0){
        rdd.repartition(1).saveAsTextFile("s3n://" + s3bucket + "/" + Calendar.getInstance().getTime())
      }
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}