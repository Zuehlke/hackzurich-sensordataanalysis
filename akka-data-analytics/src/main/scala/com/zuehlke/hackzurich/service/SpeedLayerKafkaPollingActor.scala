package com.zuehlke.hackzurich.service

import java.util.{Collections, Properties}

import akka.actor.{Actor, ActorRef}
import com.zuehlke.hackzurich.common.dataformats.AccelerometerReadingJSON4S
import com.zuehlke.hackzurich.common.kafkautils.{MesosKafkaBootstrapper, Topics}
import com.zuehlke.hackzurich.service.SparkDataAnalyticsPollingActor.PollFromKafka
import com.zuehlke.hackzurich.service.SpeedLayerKafkaPollingActor.SpeedLayerData
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConversions._
import scala.collection.mutable


class SpeedLayerKafkaPollingActor(val predictionActor: ActorRef) extends Actor {
  val consumerProps = new Properties()
  consumerProps.put("bootstrap.servers", MesosKafkaBootstrapper.mkBootstrapServersString)
  consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  consumerProps.put("group.id", s"akka-${Topics.SENSOR_READING_ACCELEROMETER}-group")

  val consumer = new KafkaConsumer[String, String](consumerProps)
  consumer.subscribe(Collections.singletonList(Topics.SENSOR_READING_ACCELEROMETER))

  def readFromKafka(): mutable.ArrayBuffer[AccelerometerReadingJSON4S] = {
    val data = mutable.ArrayBuffer.empty[AccelerometerReadingJSON4S]
    val records = consumer.poll(300)
    for (r <- records.iterator()) {
      val accelerometerData: AccelerometerReadingJSON4S = AccelerometerReadingJSON4S(r.value())
      println("SpeedLayerKafkaPollingActor: Got this from Kafka: " + accelerometerData)
      data += accelerometerData
    }
    data
  }

  override def receive: Receive = {
    case PollFromKafka() =>
      predictionActor ! SpeedLayerData(readFromKafka())
    case x => println(s"SpeedLayerKafkaPollingActor: I got a weird message: $x")
  }


  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
    consumer.close()
  }
}

object SpeedLayerKafkaPollingActor {

  case class SpeedLayerData(data: Seq[AccelerometerReadingJSON4S])

}
