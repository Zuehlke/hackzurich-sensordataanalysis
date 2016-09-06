import com.zuehlke.hackzurich.RestIngestionSpec
import dispatch.Defaults._
import dispatch._
import org.junit.runner.RunWith
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import org.apache.commons.lang3.StringUtils

@RunWith(classOf[JUnitRunner])
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
      response should be(s"<h1>User $user sent msg '$content' to kafka topic sensor-reading with key '123123123'!</h1>")
    }
  }

  it should "accept posts to the sensor reading path constructing key from parameters in path" in {
    val content = "some dummy data"
    val request = url(s"http://$host:$port/sensorReading/?deviceID=123123123&deviceType=iPhone%20OS")
      .as_!(user, password)
      .POST << content
    val futureResponse = Http(request OK as.String)
    futureResponse map { response =>
      response should be(s"<h1>User $user sent msg '$content' to kafka topic sensor-reading with key '123123123_iPhone OS'!</h1>")
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

  it should "abbreviate very long data in response" in {
    val content = "some" + StringUtils.repeat(" very,", 100) + " very long message"
    val expectedAbbreviation = "some very, very, very, very, very, very, very, very, very, very, very, very, very, very, very, very, very, very, very, very, very, very, very, very, very, very, very, very, very, very, very, very, ..."

    val request = url(s"http://$host:$port/sensorReading/123123123")
      .as_!(user, password)
      .POST << content
    val futureResponse = Http(request OK as.String)
    futureResponse map { response =>
      response should be(s"<h1>User $user sent msg '$expectedAbbreviation' to kafka topic sensor-reading with key '123123123'!</h1>")
    }
  }



}