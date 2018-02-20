package com.zuehlke.hackzurich.service

import java.util.{Collections, Properties}

import akka.actor.{Actor, Props}
import com.zuehlke.hackzurich.common.dataformats.Prediction
import com.zuehlke.hackzurich.common.kafkautils.{MesosKafkaBootstrapper, Topics}
import com.zuehlke.hackzurich.service.KafkaConsumerActor.RequestPrediction
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConversions._
import scala.collection.mutable


class KafkaConsumerActor extends Actor {
  val consumerProps = new Properties()
  consumerProps.put("bootstrap.servers", MesosKafkaBootstrapper.mkBootstrapServersString)
  consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  consumerProps.put("group.id", "akka-data-analytics-group")

  val consumer = new KafkaConsumer[String, String](consumerProps)
  consumer.subscribe(Collections.singletonList(Topics.DATA_ANALYTICS))

  val map = mutable.Map.empty[String, Prediction]

  def readFromKafka(): Unit = {
    val records = consumer.poll(300)
    for (r <- records.iterator()) {
      val prediction = Prediction(r.value())
      println("Got this from Kafka: " + prediction)
      map += (prediction.deviceid -> prediction)
    }
  }

  override def receive: Receive = {
    case RequestPrediction() =>
      sender() ! map.values.toSeq
      readFromKafka()
    case x => println(s"I got a weird message: $x")
  }


  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
    consumer.close()
  }
}

object KafkaConsumerActor {
  def mkProps: Props = Props[KafkaConsumerActor]

  case class RequestPrediction()

}
