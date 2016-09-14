package com.zuehlke.hackzurich

import com.zuehlke.hackzurich.common.kafkautils.MessageStream.OffsetResetConfig
import com.zuehlke.hackzurich.common.kafkautils.{MessageStream, Topics}
import dispatch._
import dispatch.Defaults._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

import scala.util.{Failure, Success}


class HTTPForwarder(user: String, password: String, endpoint: String) extends Serializable {
  def post(record: ConsumerRecord[String, String]): Future[String] = {
    val request = url("http://"+endpoint +"/" + java.net.URLEncoder.encode(record.key, "utf-8"))
      .as_!(user, password)
      .POST << record.value
    val futureResponse = Http(request OK as.String)
    futureResponse onComplete {
      case Success(content) => {
        List(1)
      }
      case Failure(t) => {
        println(s"Failed to send message to $endpoint: " + t.getMessage)
        throw t
      }
      case _ => List.empty
    }
    futureResponse
  }
}


/**
  * Forwards content of Kafka to another Sensor Ingestion Akka REST Service
  * Exploits knowledge of the working of the REST Service, by this enable to run without granting low-level access to Kafka
  *
  *
  * Run in dcos with:
  *
  * dcos spark run --submit-args="--supervise --total-executor-cores 1 --class com.zuehlke.hackzurich.KafkaToSensorIngestionForwarder <jar_location> <topics> <sensor-ingestion-endpoint> <user> <password> <batchInterval> <startPosition>"
  */
object KafkaToSensorIngestionForwarder {

  def main(args: Array[String]) {
    if (args.length < 6) {
      System.err.println(s"""
                            |Usage: KafkaToS3 <topics> <sensor-ingestion-endpoint> <user> <password> <batchInterval> <startPosition>
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <sensor-ingestion-endpoint> the URL where the Sensor Ingestion Akka REST Service is listening
                            |  <user> the username to use for HTTP Basic Auth
                            |  <password> the password to use
                            |  <batchInterval> the number of milliseconds per batch (determines latency, but also load created by task)
                            |  <startPosition> either "fromBeginning" to start with earliest entries or will start from latest.
        """.stripMargin)
      System.exit(1)
    }

    val Array(topics, endpoint, user, password, batchInterval, startPosition) = args
    val executionName = "KafkaForwarder" + Topics.md5(topics + endpoint)

    val spark = SparkSession.builder()
      .appName(executionName)
      .getOrCreate()

    // Create context with 1 second batch interval
    val ssc = new StreamingContext(spark.sparkContext, Milliseconds(batchInterval.toInt))

    val offset = startPosition match {
      case "fromBeginning" => OffsetResetConfig.Earliest
      case _ => OffsetResetConfig.Latest
    }

    val messages = MessageStream.directMessageStream(ssc, executionName, topics, offset)

    val keyFilter = MessageStream.filterKey
    val poster = new HTTPForwarder(user, password, endpoint.replaceAll("/$",""))

    val sendCount = messages
      .filter(keyFilter(_))
      .map(poster.post(_))
      .count()
      .print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}