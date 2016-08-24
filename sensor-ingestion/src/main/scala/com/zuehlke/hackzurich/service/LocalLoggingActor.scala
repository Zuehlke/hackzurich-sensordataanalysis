package com.zuehlke.hackzurich.service

import akka.actor.Props
import com.zuehlke.hackzurich.service.ProducerActor.MessagesProcessedResponse

class LocalLoggingActor extends ProducerActor {
  var messagesProcessed: Long = 0

  override def handleMessage(msg: String, topic: String, key: Option[String]): Unit = {
    log.info(s"Received message with content $msg, topic $topic and key $key.")
    messagesProcessed += 1
  }

  override def handleMessagesProcessedRequest(): Unit = {
    log.info(s"Requested count of processed messages, returning $messagesProcessed.")
    sender ! MessagesProcessedResponse( messagesProcessed )
  }
}

object LocalLoggingActor {
  def mkProps: Props = Props[LocalLoggingActor]
}