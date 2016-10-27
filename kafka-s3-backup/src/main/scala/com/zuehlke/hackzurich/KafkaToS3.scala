package com.zuehlke.hackzurich

import java.time.{ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Calendar

import com.zuehlke.hackzurich.common.kafkautils.MessageStream
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

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
                            |  <s3bucket> the s3bucket without s3://-prefix to store the files into (s3bucket may contain subfolders, will get created if needed)
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

    val groupID = "KafkaToS3" + topics + s3bucket;

    val messages = MessageStream.directMessageStream(ssc, groupID, topics)

    // Save to S3
    messages.foreachRDD(rdd => {
      val rddCount: Long = rdd.count()
      println("\n\nNumber of records in this batch : " + rddCount)
        if(rddCount > 0){
          (ssc.sparkContext.makeRDD(Seq("[")) ++
            rdd // must convert to String first because ConsumerRecord can not be easily repartitioned (no serializer)
              .map(record => {
              val valueWithoutWhitespaces: String = record.value().replaceAll("\\s+","")
                s"""{\"${record.key}\" : ${valueWithoutWhitespaces}}"""
              })
              .repartition(1)
                .zipWithIndex()
                .map(recordWithIndex =>{
                  if(rddCount-1 > recordWithIndex._2) {
                    recordWithIndex._1 + ','
                  } else {
                    recordWithIndex._1
                  }
                })
            ++ ssc.sparkContext.makeRDD(Seq("]")))
            .repartition(1)
            .saveAsTextFile("s3n://" + s3bucket + "/" + ZonedDateTime.now().withZoneSameInstant(ZoneId.of("UTC+2")).format(DateTimeFormatter.ofPattern("YYYY-MM-dd'T'HH-mm-ssZ")))
        }
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}