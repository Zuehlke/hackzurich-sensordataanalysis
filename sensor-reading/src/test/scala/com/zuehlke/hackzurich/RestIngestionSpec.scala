package com.zuehlke.hackzurich

import com.zuehlke.hackzurich.service.{LocalLoggingActor, RestIngestionLauncher}
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll}

trait RestIngestionSpec extends AsyncFlatSpec with BeforeAndAfterAll {
  val host = "localhost"
  val port = 28081
  val user = "integration-tester"
  val password = "hackzurich"

  override protected def beforeAll(): Unit = {
    RestIngestionLauncher.launchWith(LocalLoggingActor.mkProps, host, port)
  }

  override protected def afterAll(): Unit = {
    RestIngestionLauncher.tearDown()
  }
}
