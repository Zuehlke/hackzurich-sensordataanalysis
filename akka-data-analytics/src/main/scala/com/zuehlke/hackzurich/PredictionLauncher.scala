package com.zuehlke.hackzurich

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import com.zuehlke.hackzurich.configuration.HttpConfiguration
import com.zuehlke.hackzurich.routes.ExportRoute
import com.zuehlke.hackzurich.service.SparkDataAnalyticsPollingActor.PollFromKafka
import com.zuehlke.hackzurich.service.{PredictionActor, SparkDataAnalyticsPollingActor, SpeedLayerKafkaPollingActor}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

object PredictionLauncher {

  implicit val config: Config = ConfigFactory.load()
  implicit val system: ActorSystem = ActorSystem("http-data-analytics-system", config)
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  var bindingFuture: Future[Http.ServerBinding] = null
  val kafkaPollInterval: FiniteDuration = 5 seconds

  def main(args: Array[String]) {
    println(s"Starting server ingesting to DC/OS Kafka cluster")

    val predictionActor = system.actorOf(Props[PredictionActor], "prediction-actor")
    val speedLayerKafkaPollingActor = system.actorOf(Props(new SpeedLayerKafkaPollingActor(predictionActor)), "speed-layer-kafka-polling-actor")
    val sparkDataAnalyticsPollingActor = system.actorOf(Props(new SparkDataAnalyticsPollingActor(predictionActor)), "spark-data-analytics-polling-actor")

    system.scheduler.schedule(0 seconds, kafkaPollInterval, sparkDataAnalyticsPollingActor, PollFromKafka())
    system.scheduler.schedule(0 seconds, kafkaPollInterval, speedLayerKafkaPollingActor, PollFromKafka())

    val route = new ExportRoute(predictionActor, system)
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
