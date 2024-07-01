package ch.cern.sparkmeasure

import org.slf4j.{Logger, LoggerFactory}

import java.net.{HttpURLConnection, URL, URLEncoder}

/**
 * PushGateway
 * Note this does not use the prometheous client bather rather implements it using and HTTP client
 * Http Client: send metrics to prometheus pushgateway
 *
 * Example usage for pushgateway:
 * val pushGateway = PushGateway(serverIPnPort, metricsJob)
 * pushGateway.post(str_metrics, metricsType, labelName, labelValue)
 *
 * Notes:
 * Sending same metric with different number of dimensions will stop collecting data
 * from Pushgateway with error. So we send defaults if labelName and/or labelValue is empty.
 *
 * Metrics names, metricsJob, metricsType, labelName, labelValue
 * must match the format described in the document:
 * https://prometheus.io/docs/instrumenting/exposition_formats/
 * Names that can't be url-encoded will be set to default values.
 *
 * Valid characters for metrics and label names are: A-Z, a-z, digits and '_'.
 * Metric name can also contain ':'.
 * Metrics and label names cannot start with digit.
 * All non-matching characters will be replaced with '_'.
 * If some name starts with digit leading '_' will be added.
 */


/**
 * config: case class with all required configuration for pushgateway,
 */
case class PushGateway(config: PushgatewayConfig) {
  lazy val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

  private var urlJob = s"DefaultJob"
  try {
    urlJob = URLEncoder.encode(config.jobName, s"UTF-8")
  } catch {
    case _: java.io.UnsupportedEncodingException =>
      logger.error(s"metricsJob '${config.jobName}' cannot be url encoded")
  }
  val urlBase = s"http://${config.serverIPnPort}/metrics/job/$urlJob/instance/sparkMeasure"

  private val requestMethod = s"POST"
  private val connectTimeout = config.connectionTimeoutMs
  private val readTimeout = config.readTimeoutMs


  /**
   * name: name String, validChars: String with valid characters,
   * replace all other characters with '_",
   * remove leading and trailing white spaces,
   * if first symbol is digit, add leading '_".
   */
  def validateName(name: String, validChars: String): String = {

    if (name == null) return null

    val trimmedStr = name.replaceAll(validChars, s" ").trim
    var resultStr = trimmedStr.replaceAll(s"[ ]", s"_")
    if (resultStr.charAt(0).isDigit) resultStr = s"_" + resultStr
    resultStr

  }


  /**
   * name: String with label name,
   * replace all not valid characters with '_",
   * remove leading and trailing white spaces,
   * if first symbol is digit, add leading '_".
   */
  def validateLabel(name: String): String = {

    validateName(name, s"[^a-zA-Z0-9_]")

  }


  /**
   * name: String with metric name,
   * replace all not valid characters with '_",
   * remove leading and trailing white spaces,
   * if first symbol is digit, add leading '_".
   */
  def validateMetric(name: String): String = {

    validateName(name, s"[^a-zA-Z0-9_:]")

  }


  /**
   * metrics: String with metric name-value pairs each ending with eol,
   * metricsType: metrics type (task or stage),
   * labelName: metrics label name,
   * labelValue: metrics label value
   */
  def post(metrics: String, metricsType: String, labelName: String, labelValue: String): Unit = {

    var urlType = s"NoType"
    try {
      if ((metricsType != null) && (metricsType != ""))
        urlType = URLEncoder.encode(metricsType, s"UTF-8")
    } catch {
      case uee: java.io.UnsupportedEncodingException =>
        logger.warn(s"metricsType '$metricsType' cannot be url encoded, use default")
    }

    var urlLabelName = s"NoLabelName"
    if ((urlLabelName != null) && (urlLabelName != ""))
      urlLabelName = validateLabel(labelName)

    var urlLabelValue = s"NoLabelValue"
    try {
      if ((labelValue != null) && (labelValue != ""))
        urlLabelValue = URLEncoder.encode(labelValue, s"UTF-8")
    } catch {
      case uee: java.io.UnsupportedEncodingException =>
        logger.warn(s"labelValue '$labelValue' cannot be url encoded, use default")
    }
    val urlFull = urlBase + s"/type/" + urlType + s"/" + urlLabelName + s"/" + urlLabelValue

    try {
      val connection = new URL(urlFull).openConnection.asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(connectTimeout)
      connection.setReadTimeout(readTimeout)
      connection.setRequestMethod(requestMethod)
      connection.setRequestProperty("Content-Type", "text/plain; version=0.0.4")
      connection.setDoOutput(true)

      val outputStream = connection.getOutputStream
      if (outputStream != null) {
        outputStream.write(metrics.getBytes("UTF-8"))
        outputStream.flush();
        outputStream.close();
      }

      val responseCode = connection.getResponseCode
      val responseMessage = connection.getResponseMessage
      connection.disconnect();
      if (responseCode != 200 && responseCode != 202) // 200 and 202 Accepted, 400 Bad Request
        logger.error(s"Data sent error, url: '$urlFull', response: $responseCode '$responseMessage'")
    } catch {
      case ste: java.net.SocketTimeoutException =>
        println("java.net.SocketTimeoutException")
        logger.error(s"Data sent error, url: '$urlFull', " + ste.getMessage)
      case ioe: java.io.IOException =>
        println("java.io.IOException")
        logger.error(s"Data sent error, url: '$urlFull', " + ioe.getMessage)
    }
  }
}
