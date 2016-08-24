package com.zuehlke.hackzurich

import com.zuehlke.hackzurich.configuration.RestIngestionConfiguration
import com.zuehlke.hackzurich.service.{LocalLoggingActor, RestIngestionLauncher}

object RestIngestionLocalLauncher {

  def main(args: Array[String]) {
    println(s"Starting server ingesting to local standard out")
    RestIngestionLauncher.launchWith(LocalLoggingActor.mkProps, RestIngestionConfiguration.HOSTNAME, RestIngestionConfiguration.PORT)
  }
}