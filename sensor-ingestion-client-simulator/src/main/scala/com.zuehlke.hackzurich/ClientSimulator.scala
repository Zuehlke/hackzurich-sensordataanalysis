package com.zuehlke.hackzurich

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import com.ning.http.client.Response
import dispatch._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

object ClientSimulator {

  def main(args: Array[String]) {
    if(!isValid(args)) {
      printUsage
    }
    val configuration: SimulatorConfiguration = getConfiguration(args)

    simulate(configuration);
  }


  def getAuth(args: Array[String]): Option[BasicAuthorization] = {
    return args
      .find(arg => arg.startsWith("--auth"))
      .map(option => option.split('=')(1))
      .map(authString =>
        BasicAuthorization(
          authString.split(':')(0),
          authString.split(':')(1)))
  }

  def getMessageCount(args: Array[String]): Int = {
    return args.find(_.startsWith("--message-count")).map(_.split('=')(1).toInt).getOrElse(1);
  }

  def getConfiguration(args: Array[String]): SimulatorConfiguration = {
    return RandomSimulatorConfiguration(ServiceUrlConfiguration(getUrl(args),getAuth(args)), getMessageCount(args));
  }

  def getUrl(args: Array[String]): String = {
    return args.filter(arg => !arg.startsWith("--"))(0);
  }


  def isValid(args: Array[String]): Boolean = {
    if(args.length - args.filter(_.startsWith("--")).length != 1){
      return false;
    }
    return isDataBasisOptionValid(args) && isAuthOptionValid(args) && isMessageCountValid(args);
  }

  def isMessageCountValid(args: Array[String]): Boolean = {
    val messageCountArgs: Array[String] = args.filter(arg => arg.startsWith("--message-count"));
    if (messageCountArgs.length == 0) {
      return true;
    }
    if(messageCountArgs.length > 1) {
      return false;
    }
    val messageCount: Array[String] = messageCountArgs(0).split('=');
    if(messageCount.length != 2 || messageCount(1).isEmpty || !messageCount(1).forall(_.isDigit)) {
      return false;
    }
    return true;
  }

  def isAuthOptionValid(args: Array[String]): Boolean = {
    val authArgs: Array[String] = args.filter(arg => arg.startsWith("--auth"));
    if (authArgs.length == 0) {
      return true;
    }
    if(authArgs.length > 1) {
      return false;
    }
    val authArg: Array[String] = authArgs(0).split('=');
    if(authArg.length != 2 || authArg(1).isEmpty || authArg(1).split(":").length != 2) {
      return false;
    }
    return true;
  }

  def isDataBasisOptionValid(args: Array[String]): Boolean = {
    val dataBasisArgs: Array[String] = args.filter(_.startsWith("--data-basis"));
    if (dataBasisArgs.length == 0) {
      return true;
    }
    if (dataBasisArgs.length > 1) {
      return false;
    }
    val dataBasisArg: Array[String] = dataBasisArgs(0).split('=');
    if (dataBasisArg.length != 2 || dataBasisArg(1).isEmpty) {
      return false;
    }
    return true;
  }

  def simulate(configuration: SimulatorConfiguration): Unit = {
    configuration match {
      case KafkaDataSourceConfiguration(_,_) => {
        // TODO
      }
      case RandomSimulatorConfiguration(service, messageCount) => {
        import ExecutionContext.Implicits.global
        val request: Req = (service.auth match {
          case Some(auth) => url(service.url).as_!(auth.user, auth.password)
          case _ =>  url(service.url)
        })
        val futureList: List[Future[Res]] = (1 to messageCount).toList.map(_ => sendRequest(request))
        Await.ready(Future.sequence(futureList), Duration.Inf)
      }
    }
    System.exit(0);
  }

  def sendRequest(request: Req): Future[Response] = {
    import ExecutionContext.Implicits.global
    val dateTime: String = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now())
    val future: Future[Res] = Http(request << s"""{"z" : -0.1, "x" : -0.2, "y" : 0.1, "date" : "$dateTime", "type" : "Gyro"}""")
    future onComplete {
      case Success(response) => System.out.println(s"${response.getStatusCode} : ${response.getResponseBody}")
      case Failure(t) => System.err.println(t);
    }
    future
  }

  def printUsage: Unit = {
    System.err.println(s"""
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