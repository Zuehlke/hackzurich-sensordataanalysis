package com.zuehlke.hackzurich

import com.zuehlke.hackzurich.configuration.RestIngestionConfiguration
import com.zuehlke.hackzurich.service.{KafkaProducerActor, RestIngestionLauncher}

object RestIngestionKafkaLauncher {
  def main(args: Array[String]) {
    println(s"Starting server ingesting to DC/OS Kafka cluster")
    RestIngestionLauncher.launchWith(KafkaProducerActor.mkProps, RestIngestionConfiguration.HOSTNAME, RestIngestionConfiguration.PORT)
  }
}