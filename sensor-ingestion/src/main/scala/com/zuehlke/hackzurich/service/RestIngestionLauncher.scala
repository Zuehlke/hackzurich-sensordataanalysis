package com.zuehlke.hackzurich.service

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import com.zuehlke.hackzurich.routes.SensorDataIngestionRoute

import scala.concurrent.Future

object RestIngestionLauncher {

  implicit val config = ConfigFactory.load()
  implicit val system = ActorSystem("my-system", config)
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  var bindingFuture: Future[Http.ServerBinding] = null

  def launchWith(ingestionActorProps: Props, hostname: String, port: Int): Unit = {

    val basicAuthPassword = config.getString("hackzurich.basicauth.password")
    val ingestionActor = system.actorOf(ingestionActorProps, "ingestion-actor")
    val route = new SensorDataIngestionRoute(ingestionActor, basicAuthPassword)
    bindingFuture = Http().bindAndHandle(route.route, hostname, port)

    println(s"Server online at http://$hostname:$port/")
  }

  def tearDown(): Unit = {
    if( bindingFuture != null) {
      bindingFuture
        .flatMap(_.unbind())
        .onComplete(_ => system.terminate())
    }
    bindingFuture = null
  }

}
