package ch.cern.sparkmeasure

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import org.scalatest.{FlatSpec, Matchers}

import java.net.ServerSocket

class PushGatewayTest extends FlatSpec with Matchers {

  /** Get available ip port */
  var ip_port = 0
  try {
    val socket = new ServerSocket(ip_port)
    ip_port = socket.getLocalPort
    socket.close
  }
  catch {
    case _: Throwable =>
  }

  if (ip_port == 0) {
    it should "get available ip port" in {
      (ip_port > 0) shouldEqual true
    }
  } else {

    /** Run wiremock server on local machine with the port */
    val wireMockServer = new WireMockServer(ip_port)


    /** Init class PushGateway */
    val serverIPnPort = s"localhost:" + ip_port.toString
    val metricsJob = s"aaa_job"

    val pushGateway = PushGateway(
      PushgatewayConfig(
        serverIPnPort = serverIPnPort,
        jobName = metricsJob
      )
    )


    /** Check metric name validation for Prometheus */
    it should "accept label name containing letters and underscores" in {
      pushGateway.validateLabel(s"aaa_label") shouldEqual s"aaa_label"
    }

    it should "change label name containing other symbols and trim spaces" in {
      pushGateway.validateLabel(s" #aaa(label): ") shouldEqual s"aaa_label"
    }

    it should "accept metric name containing letters, underscores and colons" in {
      pushGateway.validateMetric(s"aaa_metric:") shouldEqual s"aaa_metric:"
    }

    it should "change metric name containing other symbols and trim spaces" in {
      pushGateway.validateMetric(s" #aaa(metric) ") shouldEqual s"aaa_metric"
    }


    /** Metrics data to send */
    val metricsType = s"metricsType"
    val labelName = s"labelName"
    val labelValue = s"labelValue"
    val urlBase = s"/metrics/job/" + metricsJob + s"/instance/sparkMeasure"
    val urlFull = urlBase + s"/type/" + metricsType + s"/" + labelName + s"/" + labelValue

    val content_type = s"text/plain; version=0.0.4"
    val str_metrics = s"str_metrics"


    wireMockServer.start()

    wireMockServer.stubFor(post(urlEqualTo(urlFull))
      .withHeader("Content-Type", equalTo(content_type))
      .withRequestBody(containing(str_metrics))
      .willReturn(
        aResponse()
          .withStatus(200)
      )
    )


    /** Send metrics */
    pushGateway.post(str_metrics, metricsType, labelName, labelValue)


    /** Check sent request */
    val reqList = wireMockServer.findAll(postRequestedFor(urlEqualTo(urlFull))
      .withHeader("Content-Type", equalTo(content_type))
      .withRequestBody(containing(str_metrics))
    )

    it should "send one post request with parameters to pushGateway" in {
      reqList.size() shouldEqual 1
    }


    wireMockServer.stop()
  }

}
