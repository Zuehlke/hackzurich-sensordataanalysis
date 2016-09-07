package com.zuehlke.hackzurich.configuration

import com.zuehlke.hackzurich.common.kafkautils.Topics
import org.apache.commons.lang3.StringUtils

object RestIngestionConfiguration {
  val HOSTNAME: String = StringUtils.defaultString(System.getenv("HOSTNAME"), "localhost")
  val PORT: Int = Integer.valueOf(StringUtils.defaultString(System.getenv("PORT0"), "18080"))
  val TOPIC: String = StringUtils.defaultString(System.getenv("KAFKA_TOPIC"), Topics.SENSOR_READING)
}
