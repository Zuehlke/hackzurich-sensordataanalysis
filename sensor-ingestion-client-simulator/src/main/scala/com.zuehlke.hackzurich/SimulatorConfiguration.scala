package com.zuehlke.hackzurich

import java.io.File

sealed trait SimulatorConfiguration {
  def service: ServiceUrlConfiguration
}

case class RandomSimulatorConfiguration(service: ServiceUrlConfiguration, messageCount: Int) extends SimulatorConfiguration

case class KafkaDataSourceConfiguration(service: ServiceUrlConfiguration, source: File) extends SimulatorConfiguration

case class ServiceUrlConfiguration(url: String, auth: Option[BasicAuthorization])

case class BasicAuthorization(user: String, password: String)

object SimulatorConfigurationFactory {

  def getConfiguration(args: Array[String]): SimulatorConfiguration = {
    val service = ServiceUrlConfiguration(getUrl(args),getAuth(args))
    if(args.filter(_.startsWith("--data-basis")).length > 0){
      return KafkaDataSourceConfiguration(service,getDataBasis(args))
    }
    return RandomSimulatorConfiguration(service, getMessageCount(args))
  }

  def getDataBasis(args: Array[String]): File = {
    new File(args.filter(_.startsWith("--data-basis"))(0).split('=')(1));
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

  def getUrl(args: Array[String]): String = {
    return args.filter(arg => !arg.startsWith("--"))(0);
  }
}

object SimulatorConfigurationArgumentValidator {
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
}