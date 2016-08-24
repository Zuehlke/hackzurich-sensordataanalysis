package com.zuehlke.hackzurich.service

import akka.actor.{Actor, ActorLogging}
import com.zuehlke.hackzurich.service.ProducerActor.{Message, RequestMessagesProcessed}

trait ProducerActor extends Actor with ActorLogging {
  def handleMessage(msg: String, topic: String, key: Option[String]): Unit
  def handleMessagesProcessedRequest(): Unit

  override def receive: Receive = {
    case Message( msg, topic, key )   => handleMessage( msg, topic, key )
    case RequestMessagesProcessed     => handleMessagesProcessedRequest()
    case m                            => log.warning("received unknown message: " + m)
  }
}

object ProducerActor {
  case class Message(message: String, topic: String, key: Option[String] )
  case object RequestMessagesProcessed
  case class MessagesProcessedResponse( count: Long )
}
