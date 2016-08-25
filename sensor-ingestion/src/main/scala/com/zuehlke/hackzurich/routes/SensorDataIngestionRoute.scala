package com.zuehlke.hackzurich.routes

import akka.actor.ActorRef
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.http.scaladsl.server.directives.Credentials
import akka.pattern.ask
import akka.util.Timeout
import com.zuehlke.hackzurich.service.ProducerActor._

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.http.scaladsl.model.StatusCodes._
import com.zuehlke.hackzurich.routes.SensorDataIngestionRoute.BasicAuthPassword

class SensorDataIngestionRoute(val kafkaProducerActor: ActorRef, password: BasicAuthPassword) {
  implicit val timeout = Timeout(1 seconds)

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit def myExceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case ex =>
        extractUri { uri =>
          println(s"Request to $uri could not be handled normally")
          complete(HttpResponse(InternalServerError, entity = "That went horribly, horribly wrong"))
        }
    }

  val route =
    path("hello") {
      helloGET()
    } ~
      pathPrefix("sensorReading" / Remaining) { deviceId =>
        sensorReadingPOST(deviceId) ~ sensorReadingGET()
      }

  def helloGET() = {
    get {
      complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Sensor Ingestion Akka REST Service running</h1>"))
    }
  }

  def sensorReadingGET() = {
    get {
      authenticateBasic(realm = "get sensor reading", userPassAuthenticator) { user =>
        val futureCount = (kafkaProducerActor ? RequestMessagesProcessed).mapTo[MessagesProcessedResponse]
        val result = Await.result(futureCount, timeout.duration)
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"${result.count}"))
      }
    }
  }


  def sensorReadingPOST(deviceId: String) = {
    post {
      authenticateBasic(realm = "post sensor reading", userPassAuthenticator) { user =>
        entity(as[String]) { messageContent =>
          kafkaProducerActor ! Message(messageContent, deviceId, Some(System.currentTimeMillis().toString))
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>User $user sent msg $messageContent to kafka topic $deviceId!</h1>"))
        }
      }
    }
  }

  def userPassAuthenticator(credentials: Credentials): Option[String] =
    credentials match {
      case p@Credentials.Provided(id) if p.verify(password) => Some(id)
      case _ => None
    }
}

object SensorDataIngestionRoute {
  type BasicAuthPassword = String
}