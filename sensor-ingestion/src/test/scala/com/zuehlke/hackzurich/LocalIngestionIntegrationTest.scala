import com.ning.http.client.Realm.RealmBuilder
import com.zuehlke.hackzurich.RestIngestionSpec
import com.zuehlke.hackzurich.service.{LocalLoggingActor, RestIngestionLauncher}
import dispatch.Defaults._
import dispatch._
import org.scalatest.{Matchers, _}

class LocalIngestionIntegrationTest extends RestIngestionSpec with Matchers {
  "The rest ingestion layer" should "respond to a ping" in {
    val svc = url(s"http://$host:$port/hello")
    val futureResponse = Http(svc OK as.String)
    futureResponse map { response =>
      response should be("<h1>Sensor Ingestion Akka REST Service running</h1>")
    }
  }

  it should "produce a 401 response for the wrong basicAuth credentials" in {
    val svc = url(s"http://$host:$port/sensorReading/123123123")
    val futureResponse = Http(svc)
    futureResponse map { response =>
      response.getStatusCode should be(401)
    }
  }

  it should "respond with 0 to a request of the processed messages" in {
    val request = url(s"http://$host:$port/sensorReading/123123123")
      .as_!(user, password)
    val futureResponse = Http(request OK as.String)
    futureResponse map { response =>
      response should be("0")
    }
  }

  it should "accept posts to the sensor reading path" in {
    val content = "some dummy data"
    val request = url(s"http://$host:$port/sensorReading/123123123")
      .as_!(user, password)
      .POST << content
    val futureResponse = Http(request OK as.String)
    futureResponse map { response =>
      response should be(s"<h1>User $user sent msg $content to kafka topic 123123123!</h1>")
    }
  }


  it should "respond with a non-zero response to a request of the processed messages after posting data" in {
    val content = "some dummy data"
    val postRequest = url(s"http://$host:$port/sensorReading/123123123")
      .as_!(user, password)
      .POST << content
    val futurePostResponse = Http(postRequest OK as.String)

    val getRequest = url(s"http://$host:$port/sensorReading/123123123")
      .as_!(user, password)
    val futureGetResponse = Http(getRequest OK as.String)

    val futureMessageCount = for{
      postResponse <- futurePostResponse
      getResponse <- futureGetResponse
    } yield getResponse

    futureMessageCount map { count =>
      assert( Integer.valueOf(count) > 0 )
    }
  }


}