package com.zuehlke.hackzurich.service

import java.util.Properties

import akka.actor.Props
import com.zuehlke.hackzurich.service.ProducerActor.MessagesProcessedResponse
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class KafkaProducerActor extends ProducerActor {

  var messagesProcessed: Long = 0

  val producerProps = new Properties()
  producerProps.put("bootstrap.servers", "broker-0.kafka.mesos:9233")
  producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer =  new KafkaProducer[String, String](producerProps)

  override def handleMessage(msg: String, topic: String, key: Option[String]): Unit = {
    val record = new ProducerRecord(topic, key.getOrElse("(none)"), msg)
    producer.send( record )
    messagesProcessed += 1
  }

  override def handleMessagesProcessedRequest(): Unit = {
    sender ! MessagesProcessedResponse( messagesProcessed )
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
    producer.close()
  }
}

object KafkaProducerActor {
  def mkProps: Props = Props[KafkaProducerActor]
}