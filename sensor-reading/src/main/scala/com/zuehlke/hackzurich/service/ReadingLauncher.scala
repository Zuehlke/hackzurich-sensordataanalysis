package com.zuehlke.hackzurich.service

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import com.zuehlke.hackzurich.routes.SensorDataReadingRoute

import scala.concurrent.Future

object ReadingLauncher {

  implicit val config = ConfigFactory.load()
  implicit val system = ActorSystem("my-reading-system", config)
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  var bindingFuture: Future[Http.ServerBinding] = null

  def launchWith(readingActorProps: Props, hostname: String, port: Int): Unit = {

    val ingestionActor = system.actorOf(readingActorProps, "reading-actor")
    val route = new SensorDataReadingRoute(ingestionActor)
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
