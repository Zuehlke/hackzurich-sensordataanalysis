import com.zuehlke.hackzurich.service.{LocalLoggingActor, RestIngestionLauncher}
import dispatch.Defaults._
import dispatch._
import org.scalatest.{Matchers, _}

class LocalIngestionIntegrationTest extends AsyncFlatSpec with Matchers with BeforeAndAfter {

  before {
    RestIngestionLauncher.launchWith(LocalLoggingActor.mkProps, "localhost", 28081)
  }

  after {
    RestIngestionLauncher.tearDown()
  }


  "The rest ingestion layer" should "respond to a ping" in {
    val svc = url("http://localhost:28081/hello")
    val futureResponse = Http(svc OK as.String)
    futureResponse map { response =>
      response should be("<h1>Sensor Ingestion Akka REST Service running</h1>")
    }
  }

}