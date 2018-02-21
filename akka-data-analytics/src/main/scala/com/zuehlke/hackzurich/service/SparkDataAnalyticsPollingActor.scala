package com.zuehlke.hackzurich.service

import java.util.{Collections, Properties}

import akka.actor.{Actor, ActorRef}
import com.zuehlke.hackzurich.common.dataformats.Prediction
import com.zuehlke.hackzurich.common.kafkautils.{MesosKafkaBootstrapper, Topics}
import com.zuehlke.hackzurich.service.SparkDataAnalyticsPollingActor.{PollFromKafka, SparkAnalyticsData}
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConversions._
import scala.collection.mutable


class SparkDataAnalyticsPollingActor(val predictionActor: ActorRef) extends Actor {
  val consumerProps = new Properties()
  consumerProps.put("bootstrap.servers", MesosKafkaBootstrapper.mkBootstrapServersString)
  consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  consumerProps.put("group.id", s"akka-${Topics.DATA_ANALYTICS}-group")

  val consumer = new KafkaConsumer[String, String](consumerProps)
  consumer.subscribe(Collections.singletonList(Topics.DATA_ANALYTICS))

  def readFromKafka(): mutable.Map[String, Prediction] = {
    val map = mutable.Map.empty[String, Prediction]
    val records = consumer.poll(1000)
    for (r <- records.iterator()) {
      val prediction = Prediction(r.value())
      println("SparkDataAnalyticsPollingActor: Got this from Kafka: " + prediction)
      map += (prediction.deviceid -> prediction)
    }
    map
  }

  override def receive: Receive = {
    case PollFromKafka() =>
      predictionActor ! SparkAnalyticsData(readFromKafka().toMap)
    case x => println(s"SparkDataAnalyticsPollingActor: I got a weird message: $x")
  }


  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
    consumer.close()
  }
}


object SparkDataAnalyticsPollingActor {

  case class PollFromKafka()

  case class SparkAnalyticsData(data: Map[String, Prediction])

}