package com.zuehlke.hackzurich.routes

import java.util.Properties

import akka.actor.ActorRef
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.http.scaladsl.server.directives.{Credentials, ParameterDirectives}
import akka.pattern.ask
import akka.util.Timeout
import com.zuehlke.hackzurich.common.kafkautils.{MesosKafkaBootstrapper, Topics}

import scala.concurrent.duration._
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

/** Defines how / which requests are routed and handled
  * using the Akka HTTP Routing DSL (http://doc.akka.io/docs/akka-stream-and-http-experimental/1.0-RC3/scala/http/routing-dsl/overview.html)
  *
  * This feature is still fairly new and the DSL may still not be very well supported in your IDE.
  * So the IDE my complain about some statements that the Scala compiler within Gradle accepts - and that is the most important thing, I guess.
  */
class SensorDataReadingRoute(val producerActor: ActorRef) {
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
    path("reading") {
      sensorReadingGET()
    }

  /** As an example of what could be responded to a GET request, return the number of submitted sensor readings since last restart */
  def sensorReadingGET() = {
    get {

      val props = new Properties()
      props.put("bootstrap.servers", MesosKafkaBootstrapper.mkBootstrapServersString)
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("group.id", "sensor-reader")
      val consumer = new KafkaConsumer[String, String](props)
      val topic = "sensor-reading"
      consumer.subscribe(java.util.Arrays.asList(topic))

      val records: ConsumerRecords[String, String] = consumer.poll(1000)

        //val futureCount = (producerActor ? RequestMessagesProcessed).mapTo[MessagesProcessedResponse]
        //val result = Await.result(futureCount, timeout.duration)
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"Hihi"))

    }
  }

}

object SensorDataReadingRoute {
}