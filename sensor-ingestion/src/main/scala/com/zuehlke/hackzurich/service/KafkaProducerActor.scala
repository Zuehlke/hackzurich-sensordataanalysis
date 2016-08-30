package com.zuehlke.hackzurich.service

import java.util.Properties

import akka.actor.Props
import com.zuehlke.hackzurich.service.ProducerActor.MessagesProcessedResponse
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class KafkaProducerActor extends ProducerActor {

  var messagesProcessed: Long = 0

  val producerProps = new Properties()
  producerProps.put("bootstrap.servers", KafkaBootstrapper.mkBootstrapServersString)
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

object KafkaBootstrapper {
  import org.apache.zookeeper.ZooKeeper
  import scala.collection.JavaConversions._
  import kafka.utils.Json

  def mkBootstrapServersString : String = {
    def extractConnection(zkData: String): String = {
      val brokerInfo = Json.parseFull(zkData).get.asInstanceOf[Map[String, Any]]
      val host = brokerInfo("host").asInstanceOf[String]
      val port = brokerInfo("port").asInstanceOf[Int]
      s"$host:$port"
    }

    val zk = new ZooKeeper("master.mesos:2181/dcos-service-kafka", 10000, null)
    val ids = zk.getChildren("/brokers/ids", false)
    val connections = scala.collection.mutable.ListBuffer.empty[String]

    for (id <- ids) {
      connections += (extractConnection(new String(zk.getData("/brokers/ids/" + id, false, null))))
    }

    connections.mkString(",")
  }
}