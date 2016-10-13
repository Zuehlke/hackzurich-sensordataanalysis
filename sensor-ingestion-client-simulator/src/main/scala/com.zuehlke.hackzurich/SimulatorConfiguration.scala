package com.zuehlke.hackzurich

import java.io.File

sealed trait SimulatorConfiguration {
  def service: ServiceUrlConfiguration
}

case class RandomSimulatorConfiguration(service: ServiceUrlConfiguration, messageCount: Int) extends SimulatorConfiguration

case class KafkaDataSourceConfiguration(service: ServiceUrlConfiguration, source: File) extends SimulatorConfiguration

case class ServiceUrlConfiguration(url: String, auth: Option[BasicAuthorization])

case class BasicAuthorization(user: String, password: String)