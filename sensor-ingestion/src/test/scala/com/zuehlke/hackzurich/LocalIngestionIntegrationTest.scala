import com.zuehlke.hackzurich.service.{LocalLoggingActor, RestIngestionLauncher}
import dispatch.Defaults._
import dispatch._
import org.scalatest.{Matchers, _}

class LocalIngestionIntegrationTest extends FlatSpec with Matchers with BeforeAndAfter {

  before {
    // RestIngestionLauncher.launchWith(LocalLoggingActor.mkProps, "localhost", 28081)
  }

  after {
    // RestIngestionLauncher.tearDown()
  }


  "The rest ingestion layer" should "return something" in {
    val svc = url("http://localhost:28081/hello")
    val country = Http(svc OK as.String)
  }

}