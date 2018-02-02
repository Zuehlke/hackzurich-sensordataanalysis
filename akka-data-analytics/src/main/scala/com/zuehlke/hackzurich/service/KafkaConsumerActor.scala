package com.zuehlke.hackzurich.service

import java.util.Properties
import java.util.Collections

import akka.actor.{Actor, Props}
import com.zuehlke.hackzurich.common.kafkautils.MesosKafkaBootstrapper
import com.zuehlke.hackzurich.service.KafkaConsumerActor.{Prediction, RequestPrediction}
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConversions._

import scala.collection.mutable.ArrayBuffer


class KafkaConsumerActor extends Actor {
  val consumerProps = new Properties()
  consumerProps.put("bootstrap.servers", MesosKafkaBootstrapper.mkBootstrapServersString)
  consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  consumerProps.put("group.id", "akka-data-analytics-group")

  val consumer = new KafkaConsumer[String, String](consumerProps)
  consumer.subscribe(Collections.singletonList("data-analytics"))

  val data = ArrayBuffer.empty[Prediction]

  def readFromKafka(): Unit = {
    val records = consumer.poll(300)
    for (r <- records.iterator()) {
      val prediction = Prediction(r.topic(), r.key(), r.value())
      println("Got this from Kafka: " + prediction)
      data += prediction
    }
  }

  override def receive: Receive = {
    case RequestPrediction() =>
      sender() ! data
      readFromKafka()
    case x => println(s"I got a weird message: $x")
  }


  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
    //    consumer.close()
  }
}

object KafkaConsumerActor {
  def mkProps: Props = Props[KafkaConsumerActor]

  case class Prediction(topic: String, key: String, message: String) {
    override def toString: String = s"Topic: $topic - key: $key - value: $message"
  }

  case class RequestPrediction()

}
