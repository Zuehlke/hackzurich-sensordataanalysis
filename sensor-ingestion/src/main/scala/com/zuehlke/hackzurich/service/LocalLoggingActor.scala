package com.zuehlke.hackzurich.service

import akka.actor.Props

class LocalLoggingActor extends ProducerActor {

  override def handleMessage(msg: String, topic: String, key: Option[String]): Unit = {
    log.info(s"Received message with content (size: ${msg.length}) $msg, topic $topic and key $key.")
    IngestionStatisticsManager.updateStatistics(msg.length)
  }

  override def handleMessagesProcessedRequest(): Unit = {
    val stats = IngestionStatisticsManager.statistics
    log.info(s"Requested count of processed messages, returning $stats.")
    sender ! stats
  }
}

object LocalLoggingActor {
  def mkProps: Props = Props[LocalLoggingActor]
}