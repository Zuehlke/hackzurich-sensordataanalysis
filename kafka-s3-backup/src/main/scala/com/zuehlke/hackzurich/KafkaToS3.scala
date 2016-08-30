package com.zuehlke.hackzurich

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
  * Consumes messages from one or more topics in Kafka and puts them into an S3 bucket.
  * Usage: KafkaToS3 <brokers> <topics>
  *   <brokers> is a list of one or more Kafka brokers
  *   <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
  *    topic1,topic2
  */
object KafkaToS3 {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(s"""
                            |Usage: KafkaToS3 <brokers> <topics> <awsAccessKeyId> <awsAccesKeySecret>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <awsAccessKeyId> the aws access key
                            |  <awsAccesKeySecret> the aws access secret
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics, awsId, awsSecret) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaToS3")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    ssc.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId",awsId)
    ssc.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey",awsSecret)

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Save to S3
    messages.foreachRDD(rdd => {
      //rdd.repartition(1).saveAsTextFile("s3n://zuehlkesensordata/akkaDump")

      println("\n\nNumber of records in this batch : " + rdd.count())
      rdd.collect().foreach(println)
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
