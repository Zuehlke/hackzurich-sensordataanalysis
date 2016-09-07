package com.zuehlke.hackzurich.routes

import akka.actor.ActorRef
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.http.scaladsl.server.directives.{Credentials,ParameterDirectives}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._
import com.zuehlke.hackzurich.configuration.RestIngestionConfiguration
import com.zuehlke.hackzurich.routes.SensorDataIngestionRoute.BasicAuthPassword
import com.zuehlke.hackzurich.service.ProducerActor.{Message, MessagesProcessedResponse, RequestMessagesProcessed}
import org.apache.commons.lang3.StringUtils

/** Defines how / which requests are routed and handled
  * using the Akka HTTP Routing DSL (http://doc.akka.io/docs/akka-stream-and-http-experimental/1.0-RC3/scala/http/routing-dsl/overview.html)
  *
  * This feature is still fairly new and the DSL may still not be very well supported in your IDE.
  * So the IDE my complain about some statements that the Scala compiler within Gradle accepts - and that is the most important thing, I guess.
  */
class SensorDataIngestionRoute(val producerActor: ActorRef, password: BasicAuthPassword) {
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
      pathPrefix("sensorReading" / Remaining) { expliciteKey =>
        sensorReadingPOST(expliciteKey) ~ sensorReadingGET()
      }

  /** very basic interaction, mainly used as helathcheck whether service is up and running */
  def helloGET() = {
    get {
      complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Sensor Ingestion Akka REST Service running</h1>"))
    }
  }

  /** As an example of what could be responded to a GET request, return the number of submitted sensor readings since last restart */
  def sensorReadingGET() = {
    get {
      authenticateBasic(realm = "get sensor reading", userPassAuthenticator) { user =>
        val futureCount = (producerActor ? RequestMessagesProcessed).mapTo[MessagesProcessedResponse]
        val result = Await.result(futureCount, timeout.duration)
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"${result.count}"))
      }
    }
  }


  /** Most important operation: Record sensor readings submitted as POST request by storing it in a (Kafka) topic. */
  def sensorReadingPOST(expliciteKey: String) = {
    post {
      parameters("deviceID" ? "", "deviceType" ? "" ) { (deviceID, deviceType) =>
        val key = List(expliciteKey, deviceID, deviceType).filter(_.nonEmpty).mkString("_")
        authenticateBasic(realm = "post sensor reading", userPassAuthenticator) { user =>
          entity(as[String]) { messageContent =>
            producerActor ! Message(messageContent, RestIngestionConfiguration.TOPIC, Option(key))
            val messageSnippet = StringUtils.abbreviate(messageContent, 200)
            complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>User $user sent msg '$messageSnippet' to kafka topic ${RestIngestionConfiguration.TOPIC} with key '$key'!</h1>"))
          }
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