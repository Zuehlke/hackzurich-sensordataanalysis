package com.zuehlke.hackzurich.routes

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.pattern.ask
import akka.util.Timeout
import com.zuehlke.hackzurich.service.PredictionActor._

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
    pathSingleSlash {
      predictionGET(RequestPrediction())
    } ~
      path("combined") {
        predictionGET(RequestCombinedPrediction())
      } ~
      path("speedLayerData") {
        predictionGET(RequestSpeedLayerData())
      } ~
      path("speedLayer") {
        predictionGET(RequestSpeedLayerPrediction())
      } ~
      path("sparkData") {
        predictionGET(RequestSparkDataAnalyticsPrediction())
      } ~ path("hello") {
      helloGET()
    }

  /** very basic interaction, mainly used as helathcheck whether service is up and running */
  def helloGET(): Route = {
    get {
      complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Sensor Prediction Akka REST Service running</h1>"))
    }
  }

  def predictionGET(actorRequestMessage: PredictionActorRequests): Route = {
    get {
      val future = (predictionActor ? actorRequestMessage).mapTo[String]
      val result = Await.result(future, timeout.duration)
      val message = result.replace("\n", "<br/>\n")
      //      println("Message: " + message)
      complete(HttpEntity(ContentTypes.`text/html(UTF-8)`,
        message)
      )
    }
  }

}
