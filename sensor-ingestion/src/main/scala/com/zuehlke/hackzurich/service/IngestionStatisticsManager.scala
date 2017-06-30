package com.zuehlke.hackzurich.service

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import kamon.Kamon

/**
  * Manages and updates the statistics of the ingestion
  *
  * - Message counter
  * - Messages / second
  * - Bytes / second
  */
object IngestionStatisticsManager {
  private val updatingThreshold = 1000

  private var count: Long = 0
  private var lastCount: Long = 0
  private var lastTimestamp: Long = System.currentTimeMillis()
  private var bytesProcessed: Long = 0
  private var lastBytesProcessed: Long = 0
  private var messagesPerSecond: Double = 0
  private var kiloBytesPerSecond: Double = 0
  private var lastUpdated: String = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now())

  // Histogram to be exposed via JMX
  private val histogramMessagesPerSec = Kamon.metrics.histogram("custom-messages-per-second")
  private val histogramKBPerSec = Kamon.metrics.histogram("custom-kbytes-per-second")

  /**
    * Updates the statistic.
    *
    * A call to this function indicates, that one message has been processed by the system.
    *
    * @param bytes The size of the processed message
    */
  def updateStatistics(bytes: Int): Unit = {
    count += 1
    bytesProcessed += bytes

    if (count % updatingThreshold == 0) {
      val timeDiff = System.currentTimeMillis() - lastTimestamp
      messagesPerSecond = (count - lastCount).asInstanceOf[Double] / timeDiff * 1000 // multiplied with 1000 for seconds
      kiloBytesPerSecond = (bytesProcessed - lastBytesProcessed).asInstanceOf[Double] / timeDiff // multiplied with 1000 for seconds, divided by 1000 for KB

      lastCount = count
      lastBytesProcessed = bytesProcessed
      lastUpdated = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now())
      lastTimestamp = System.currentTimeMillis()

      histogramMessagesPerSec.record(messagesPerSecond.toLong)
      histogramKBPerSec.record(kiloBytesPerSecond.toLong)
    }
  }

  /**
    * Creates a case class of type IngestionStatistics with the latest values.
    */
  def statistics: IngestionStatistics = {
    IngestionStatistics(count, f"$messagesPerSecond%1.0f", f"$kiloBytesPerSecond%1.2f", lastUpdated)
  }
}

case class IngestionStatistics(messageCount: Long, messagesPerSecond: String, kiloBytesPerSecond: String, lastUpdated: String) {
  override def toString(): String = {
    s"IngestionStatistics(messageCount = $messageCount, messagesPerSecond = $messagesPerSecond, kiloBytesPerSecond = $kiloBytesPerSecond, lastUpdated = $lastUpdated)"
  }
}