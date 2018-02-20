package com.zuehlke.hackzurich.routes

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.pattern.ask
import akka.util.Timeout
import com.zuehlke.hackzurich.common.dataformats.Prediction
import com.zuehlke.hackzurich.service.PredictionActor.RequestPrediction

import scala.concurrent.Await
import scala.concurrent.duration._

class ExportRoute(val predictionActor: ActorRef, val system: ActorSystem) {
  implicit val timeout: Timeout = Timeout(5 seconds)

  implicit def myExceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case ex =>
        extractUri { uri =>
          println(s"Request to $uri could not be handled normally")
          complete(HttpResponse(InternalServerError, entity = "That went horribly, horribly wrong"))
        }
    }

  val route: Route =
    path("hello") {
      helloGET()
    } ~
      path("prediction") {
        predictionGET()
      }

  /** very basic interaction, mainly used as helathcheck whether service is up and running */
  def helloGET(): Route = {
    get {
      complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Sensor Prediction Akka REST Service running</h1>"))
    }
  }

  def predictionGET(): Route = {
    get {
      val future = (predictionActor ? RequestPrediction()).mapTo[Seq[Prediction]]
      val result = Await.result(future, timeout.duration)
      println("Result size: " + result.size)
      val message = result.map(p => p.toString).mkString("<br/>")
      println("Message: " + message)
      complete(HttpEntity(ContentTypes.`text/html(UTF-8)`,
        message)
      )
    }
  }

}
