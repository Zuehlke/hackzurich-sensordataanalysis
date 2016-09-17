package com.zuehlke.hackzurich

import com.zuehlke.hackzurich.configuration.RestReadingConfiguration
import com.zuehlke.hackzurich.service.{KafkaProducerActor, ReadingLauncher}

object RestReadingLauncher {
  def main(args: Array[String]) {
    println(s"Starting server ingesting to DC/OS Kafka cluster")
    ReadingLauncher.launchWith(KafkaProducerActor.mkProps, RestReadingConfiguration.HOSTNAME, RestReadingConfiguration.PORT)
  }
}