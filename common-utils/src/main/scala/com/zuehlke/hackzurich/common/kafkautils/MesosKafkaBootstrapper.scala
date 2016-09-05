package com.zuehlke.hackzurich.common.kafkautils

/** Reads from ZooKeeper the broker information for Kafka stored by DC/OS */
object MesosKafkaBootstrapper {
  import kafka.utils.Json
  import org.apache.zookeeper.ZooKeeper

  import scala.collection.JavaConversions._

  /**
    * Builds the list of Kafka brokers to bootstrap
    * in a similar way as kafka.cluster.Broker#createBroker(int, java.lang.String)
    * in org.apache.kafka:kafka_2.11:0.10.0.1
    *
    * @return comma-separated list of brokers found in ZooKeeper
    */
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