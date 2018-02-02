package com.zuehlke.hackzurich

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import com.zuehlke.hackzurich.configuration.HttpConfiguration
import com.zuehlke.hackzurich.routes.ExportRoute
import com.zuehlke.hackzurich.service.KafkaConsumerActor

import scala.concurrent.{ExecutionContextExecutor, Future}

object PredictionLauncher {

  implicit val config: Config = ConfigFactory.load()
  implicit val system: ActorSystem = ActorSystem("http-data-analytics-system", config)
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  var bindingFuture: Future[Http.ServerBinding] = null

  def main(args: Array[String]) {
    println(s"Starting server ingesting to DC/OS Kafka cluster")

    val exportActor = system.actorOf(KafkaConsumerActor.mkProps, "export-actor")
    val route = new ExportRoute(exportActor, system)
    bindingFuture = Http().bindAndHandle(route.route, HttpConfiguration.HOSTNAME, HttpConfiguration.PORT)

    println(s"Server online at http://${HttpConfiguration.HOSTNAME}:${HttpConfiguration.PORT}/")
  }

  def tearDown(): Unit = {
    if (bindingFuture != null) {
      bindingFuture
        .flatMap(_.unbind())
        .onComplete(_ => system.terminate())
    }
    bindingFuture = null
  }

}
