# sparkMeasure
## Export metrics to the Prometheus monitoring system

Batch jobs send metrics to an intermediary job which Prometheus can scrape.
The Prometheus Pushgateway can accept collected data and keep it for Prometheus to scrape.
* Related info:
   - [Prometheus monitoring system](https://prometheus.io/)
   - [Pushing metrics to Prometheus](https://prometheus.io/docs/instrumenting/pushing/)

### sparkMeasure has built-in class PushGateway to send metrics to Prometheus via Pushgateway ###

**Parameters:**

* serverIPnPort: String with Prometheus Pushgateway hostIP:Port,
* metricsJob: job name,
* labelName: metrics label name,
* labelValue: metrics label value

**Notes:**

 * Sending same metric with different number of dimensions will stop collecting data from Pushgateway with error. So we send defaults if labelName and/or labelValue is empty.
 * Metrics names, labelName, labelValue must match the format described in the document:
https://prometheus.io/docs/instrumenting/exposition_formats/
 * Names that can't be url-encoded will be set to default values.
 * Valid characters for metrics and label names are: A-Z, a-z, digits and '_'.
 * Metric name can also contain ':'.
 * Metrics and label names cannot start with digit.
 * All non-matching characters will be replaced with '_'.
 * If some name starts with digit leading '_' will be added.

**Examples:**
 
- Start the prometheus gateway. For testing, you can use:
`docker run -d -p 9091:9091 prom/pushgateway`

1. Measure metrics at the Stage level (example in Scala):
```
bin/spark-shell --packages ch.cern.sparkmeasure:spark-measure_2.13:0.25 

val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark) 
stageMetrics.begin()

...execute one or more Spark jobs...

stageMetrics.end()
stageMetrics.sendReportPrometheus(s"localhost:9091", s"aaa_job", s"label_name", s"label_value")
```

2. Short syntax can be used to set spark application name as label_name and application id as label_value :
```
stageMetrics.sendReportPrometheus(s"localhost:9091", s"aaa_job")
```

3. Check the stored metrics
point the browser to `http://localhost:9091`

---
**Additional info on PushGateway implementation:**

case class PushGateway(serverIPnPort: String, metricsJob: String)
   * Http Client: send metrics to prometheus pushgateway

Methods:
   * def validateLabel(name: String): String -> validates label name, replace not valid symbols
   * def validateMetric(name: String): String -> validates metric name, replace not valid symbols
   * def post(metrics: String, metricsType: String, labelName: String, labelValue: String): Unit -> post metrics to prometheus.

**Additional info on Stage Metrics:**

case class StageMetrics(sparkSession: SparkSession)
   * Helper class to help in collecting and storing performance metrics.

Added method:
   * def sendReportPrometheus(serverIPnPort: String, metricsJob: String,
     labelName: String = sparkSession.sparkContext.appName,
     labelValue: String = sparkSession.sparkContext.applicationId): Unit -> send metrics to prometheus pushgateway

**Additional info on Task Metrics:**

case class TaskMetrics(sparkSession: SparkSession)
   * Collects metrics at the end of each Task

Added method:
   * def sendReportPrometheus(serverIPnPort: String, metricsJob: String,
     labelName: String = sparkSession.sparkContext.appName,
     labelValue: String = sparkSession.sparkContext.applicationId): Unit -> send metrics to prometheus pushgateway
   
