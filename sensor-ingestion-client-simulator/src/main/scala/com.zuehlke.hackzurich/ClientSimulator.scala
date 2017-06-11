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
import scala.io.Source
import scala.util.{Failure, Success}

/**
  * Simulates a client with either random or real data.
  */
object ClientSimulator {

  var millisOffset = 0

  def main(args: Array[String]) {
    if (!SimulatorConfigurationArgumentValidator.isValid(args)) {
      printUsage()
    }
    val configuration: SimulatorConfiguration = SimulatorConfigurationFactory.getConfiguration(args)
    simulate(configuration)
  }

  /**
    * Performs the simulation.
    *
    * @param configuration Configuration of which data to use.
    */
  def simulate(configuration: SimulatorConfiguration): Unit = {
    import ExecutionContext.Implicits.global
    configuration match {
      case KafkaDataSourceConfiguration(service, jsonSourceRoot) =>
        val jsonFiles: List[File] = collectJsonFiles(jsonSourceRoot, ".data")
        val futureList: List[Future[Int]] = jsonFiles.flatMap(sendRequestsPreformatted(service, _))
        val ready = Await.result(Future.sequence(futureList), Duration.Inf)
        System.out.println(s"${ready.count(_ == 200)} of total ${ready.size} request were successfull (status code 200)")
      case RandomSimulatorConfiguration(service, messageCount) =>
        val request: Req = service.auth match {
          case Some(auth) => url(service.url).as_!(auth.user, auth.password)
          case _ => url(service.url)
        }
        val dateTime: String = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now())
        val futureList: List[Future[Res]] = (1 to messageCount).toList.map { _ =>
          millisOffset = millisOffset + 1
          sendRequest(request << s"""{"z" : -0.1, "x" : -0.2, "y" : 0.1, "date" : "${dateTime + millisOffset}", "type" : "Gyro"}""")
        }
        Await.ready(Future.sequence(futureList), Duration.Inf)
    }
    System.exit(0)
  }

  /**
    * Collects all JSON files and returns a list of File
    *
    * @param file          Starting directory for parsing
    * @param fileExtension File extension of the data to parse
    * @return List of files to parse later on
    */
  def collectJsonFiles(file: File, fileExtension: String = ".json"): List[File] = {
    file match {
      case f: File if f.isDirectory => file.listFiles().flatMap(collectJsonFiles(_, fileExtension)).toList
      case f: File if f.isFile && f.getName.endsWith(fileExtension) => List(file)
      case _ => Nil
    }
  }


  /**
    * Sends the request using the default JSON Parser.
    * Deprecated, please use the preformatted data version.
    */
  @Deprecated
  def sendRequests(service: ServiceUrlConfiguration, file: File): List[Future[Int]] = {
    val reader = new JSONObjectReader(new BufferedReader(new FileReader(file)))
    var next = reader.readNext()
    var futures = new ListBuffer[Future[Int]]
    while (next.nonEmpty) {
      processRequest(service, futures, next.get._1, next.get._2, true)
      next = reader.readNext()
    }
    reader.close()
    futures.toList
  }


  /**
    * Reads form a given file and performs the request
    *
    * @param service Configuration containing the url and the authentication credentials
    * @param file    File to read from
    * @return List of Future
    */
  def sendRequestsPreformatted(service: ServiceUrlConfiguration, file: File): List[Future[Int]] = {
    import ExecutionContext.Implicits.global
    var futures = new ListBuffer[Future[Int]]
    for (line <- Source.fromFile(file).getLines()) {
      val split = line.split(";")
      val name = split(0)
      val json = split(1)

      processRequest(service, futures, name, json)
    }
    futures.toList
  }

  /**
    * Takes one line of data and submits the request
    */
  private def processRequest(service: ServiceUrlConfiguration, futures: ListBuffer[Future[Int]], name: String, json: String, printSuccessMessage: Boolean = false) = {
    import ExecutionContext.Implicits.global
    Thread.sleep(20)
    val path = URLEncoder.encode(name, "utf-8").replaceAll("\\+", "%20")
    val request: Req = service.auth match {
      case Some(auth) => url(service.url + path).as_!(auth.user, auth.password)
      case _ => url(service.url + path)
    }
    val future: Future[Res] = Http(request << json)
    future onComplete {
      case Success(response) if printSuccessMessage => System.out.println(s"${response.getStatusCode} : ${response.getResponseBody}")
      case Failure(t) => System.err.println(t)
      case _ =>
    }
    futures += future map { response => response.getStatusCode }
  }

  /**
    * Sends the request with random data
    */
  def sendRequest(request: Req): Future[Response] = {
    import ExecutionContext.Implicits.global
    val future: Future[Res] = Http(request)
    future onComplete {
      case Success(response) => System.out.println(s"${response.getStatusCode} : ${response.getResponseBody}")
      case Failure(t) => System.err.println(t)
    }
    future
  }

  /**
    * Prints the usage for the ClientSimulator
    */
  def printUsage(): Unit = {
    System.err.println(
      s"""
         |Usage: ClientSimulator <options> <service url>
         | when --data-basis is not provided, random data will be generated and sent
         | <options>
         |  --auth=user:password\t\t\tuses basic auth provided
         |  --message-count=<count>\t\t\tthe amount of random data sent. (not effect on --data-basis)
         |  --data-basis=<folder/filer>\t\t\tsends the kafka-captured data in the provided folder or file
        """.stripMargin)
    System.exit(1)
  }
}