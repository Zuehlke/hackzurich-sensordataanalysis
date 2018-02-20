package com.zuehlke.hackzurich.common.kafkautils

import java.security.MessageDigest

object Topics {
  val SENSOR_READING = "sensor-reading"
  val SENSOR_READING_ACCELEROMETER = "sensor-reading-accelerometer"
  val DATA_ANALYTICS = "data-analytics"

  /**
    * convenience function to generate an MD5 hash from a topic name - or any other string,
    *   helping to come up with (almost) unique names and consumer group IDs from the topic.
    */
  def md5(s: String) = {
    MessageDigest.getInstance("MD5").digest(s.getBytes)
  }
}
