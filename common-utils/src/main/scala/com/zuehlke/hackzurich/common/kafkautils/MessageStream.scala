package com.zuehlke.hackzurich.common.kafkautils

import com.zuehlke.hackzurich.common.kafkautils.MessageStream.OffsetRestConfig.OffsetRestConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Row, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object MessageStream {
  object OffsetRestConfig extends Enumeration {
    type OffsetRestConfig = Value
    val Earliest, Latest, None = Value
  }

  def directMessageStream(ssc : StreamingContext, consumerGroupID : String,
                          topics : String = Topics.SENSOR_READING, startAt : OffsetRestConfig = OffsetRestConfig.Latest)
  : InputDStream[ConsumerRecord[String, String]] = {
    // Create direct kafka stream with brokers and topics
    val kafkaBrokers = MesosKafkaBootstrapper.mkBootstrapServersString
    val topicsSet = topics.split(",").toSet

    // although we assume topics to be generic, we need to assume or make configurable the types of keys/values and corresponding deserializers
    val defaultDeserializer = "org.apache.kafka.common.serialization.StringDeserializer"

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBrokers,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> startAt.toString.toLowerCase,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> defaultDeserializer,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> defaultDeserializer,
      ConsumerConfig.GROUP_ID_CONFIG -> consumerGroupID
    )

    KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
  }

  def parseJson(ssql: SQLContext, sc: SparkContext, wholeJson: String) : Dataset[Row]= {
    ssql.read.json(sc.makeRDD(Array(wholeJson)))
  }
}