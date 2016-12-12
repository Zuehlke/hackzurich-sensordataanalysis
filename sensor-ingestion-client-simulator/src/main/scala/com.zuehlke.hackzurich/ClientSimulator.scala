package com.zuehlke.hackzurich

import java.io.{BufferedReader, File, FileReader}
import java.net.URLEncoder
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import com.ning.http.client.Response
import dispatch._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

object ClientSimulator {

  def main(args: Array[String]) {
    if (!SimulatorConfigurationArgumentValidator.isValid(args)) {
      printUsage
    }
    val configuration: SimulatorConfiguration = SimulatorConfigurationFactory.getConfiguration(args)

    simulate(configuration);
  }

  def simulate(configuration: SimulatorConfiguration): Unit = {
    import ExecutionContext.Implicits.global
    configuration match {
      case KafkaDataSourceConfiguration(service, jsonSourceRoot) => {
        val jsonFiles: List[File] = collectJsonFiles(jsonSourceRoot)
        val futureList: List[Future[Int]] = jsonFiles.flatMap(sendRequests(service, _))
        Await.ready(Future.sequence(futureList), Duration.Inf)
      }
      case RandomSimulatorConfiguration(service, messageCount) => {
        val request: Req = (service.auth match {
          case Some(auth) => url(service.url).as_!(auth.user, auth.password)
          case _ => url(service.url)
        })
        val dateTime: String = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now())
        val futureList: List[Future[Res]] = (1 to messageCount).toList.map(_ => sendRequest(request << s"""{"z" : -0.1, "x" : -0.2, "y" : 0.1, "date" : "$dateTime", "type" : "Gyro"}"""))
        Await.ready(Future.sequence(futureList), Duration.Inf)
      }
    }
    System.exit(0);
  }

  def collectJsonFiles(file: File): List[File] = {
    file match {
      case f: File if f.isDirectory => file.listFiles().flatMap(collectJsonFiles(_)).toList
      case f: File if f.isFile && f.getName().endsWith(".json") => List(file)
      case _ => Nil
    }
  }

  def sendRequests(service: ServiceUrlConfiguration, file: File): List[Future[Int]] = {
    import ExecutionContext.Implicits.global
    val reader = new JSONObjectReader(new BufferedReader(new FileReader(file)))
    var next = reader.readNext()
    var futures = new ListBuffer[Future[Int]];
    while(next.nonEmpty){
      Thread.sleep(20)
      val path = URLEncoder.encode(next.get._1,"utf-8").replaceAll("\\+", "%20")
      val request: Req = (service.auth match {
        case Some(auth) => url(service.url + path).as_!(auth.user, auth.password)
        case _ => url(service.url + path)
      })
      val future: Future[Res] = Http(request <<  next.get._2)
      future onComplete {
        case Success(response) => System.out.println(s"${response.getStatusCode} : ${response.getResponseBody}")
        case Failure(t) => System.err.println(t);
      }
      futures += future map { response => response.getStatusCode }
      next = reader.readNext()
    }
    reader.close()
    futures.toList
  }

  def sendRequest(request: Req): Future[Response] = {
    import ExecutionContext.Implicits.global
    val future: Future[Res] = Http(request)
    future onComplete {
      case Success(response) => System.out.println(s"${response.getStatusCode} : ${response.getResponseBody}")
      case Failure(t) => System.err.println(t);
    }
    future
  }

  def printUsage: Unit = {
    System.err.println(
      s"""
         |Usage: ClientSimulator <options> <service url>
         | when --data-basis is not provided, random data will be generated and sent
         | <options>
         |  --auth=user:password\t\t\tuses basic auth provided
         |  --message-count=<count>\t\t\tthe amount of random data sent. (not effect on --data-basis)
         |  --data-basis=<folder/filer>\t\t\tsends the kafka-captured data in the provided folder or file
        """.stripMargin);
    System.exit(1);
  }
}