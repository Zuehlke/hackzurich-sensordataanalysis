package com.zuehlke.hackzurich.routes

import akka.actor.ActorRef
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives._
import akka.pattern.ask
import akka.util.Timeout
import com.zuehlke.hackzurich.service.ProducerActor._

import scala.concurrent.Await
import scala.concurrent.duration._

class SensorDataIngestionRoute(val kafkaProducerActor: ActorRef ) {
  implicit val timeout = Timeout(1 seconds)
  import scala.concurrent.ExecutionContext.Implicits.global

  val route =
    path("hello") {
      get {
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Sensor Ingestion Akka REST Service running</h1>"))
      }
    } ~
    pathPrefix("sensorReading" / Remaining) { deviceId =>
      post {
        entity( as[String] ) { messageContent =>
          kafkaProducerActor ! Message(messageContent, deviceId, Some(System.currentTimeMillis().toString))
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>Sent msg to kafka topic $deviceId!</h1>"))
        }
      } ~
      get {
        val futureCount = (kafkaProducerActor ? RequestMessagesProcessed).mapTo[MessagesProcessedResponse] recover {
          case m => MessagesProcessedResponse(-1)
        }
        val result = Await.result(futureCount, timeout.duration)
        val responseCode = if (result.count >= 0) { 200 } else { 500 }
        complete(responseCode, HttpEntity(ContentTypes.`text/html(UTF-8)`, s"${result.count}"))
      }
    }
}

