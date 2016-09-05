package com.zuehlke.hackzurich

import kafka.utils.Json


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
