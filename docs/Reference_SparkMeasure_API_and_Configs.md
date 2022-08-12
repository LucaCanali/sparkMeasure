# Reference Guide to SparkMeasure APIs and Configurations

This doc is a reference to the sparkMeasure API, modules, and configuration parameters.  
Contents:  
- [StageMetrics - Scala](#stagemetrics---scala)
- [StageMetrics - Python](#stagemetrics---python)
- [StageInfoRecorderListener](#stageinforecorderlistener)
- [TaskMetrics - Scala](#taskmetrics---scala)
- [TaskInfoRecorderListener](#taskinforecorderlistener)
- [TaskMetrics - Python](#taskmetrics---python)
- [Flight Recorder Mode - File Sink](#flight-recorder-mode---file-sink)
- [InfluxDBSink and InfluxDBSinkExtended](#influxdbsink-and-influxdbsinkextended)
- [KafkaSink and KafkaSinkExtended](#kafkasink-and-kafkasinkextended)
- [IOUtils](#ioutils)
- [Utils](#utils)

## StageMetrics - Scala

```
  case class StageMetrics(sparkSession: SparkSession)

  Stage Metrics: collects stage-level metrics with Stage granularity
  and provides aggregation and reporting functions for the end-user
  
  Example usage for stage metrics:
  val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)
  stageMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show)

Configuration:

spark.sparkmeasure.stageinfo.verbose, boolean, default true
   Note: this control print the stage info report and collecting executor memory metrics  
spark.sparkmeasure.stageinfo.executormetrics, string, default "JVMHeapMemory,OnHeapExecutionMemory")
   Note: this is the list of executor metrics that are captured
   documentation on the available metrics at: https://spark.apache.org/docs/latest/monitoring.html#executor-metrics
```

Methods:  
```
def begin(): Long // Marks the beginning of data collection
def end(): Long  // Marks the end of data collection
def removeListener(): Unit // helper method to remove the listener

// Compute basic aggregation on the Stage metrics for the metrics report
// also filter on the time boundaries for the report
def aggregateStageMetrics() : LinkedHashMap[String, Long] 

// Extracts stages and their duration
def stagesDuration() : LinkedHashMap[Int, Long]

// Custom aggregations and post-processing of metrics data
def report(): String

// Runs report and prints it
def printReport(): Unit

// Custom aggregations and post-processing of executor metrics data with memory usage details
// Note this report requires per-stage memory (executor metrics) data which is sent by the executors
// at each heartbeat to the driver, there could be a small delay or the order of a few seconds
// between the end of the job and the time the last metrics value is received
// if you receive the error message java.util.NoSuchElementException: key not found,
// retry running the report after waiting for a few seconds.
def reportMemory(): String

// Runs the memory report and prints it
def printMemoryReport(): Unit

// Legacy transformation of data recorded from the custom Stage listener
// into a DataFrame and register it as a view for querying with SQL
def createStageMetricsDF(nameTempView: String = "PerfStageMetrics"): DataFrame

// Legacy metrics aggregation computed using SQL
def aggregateStageMetrics(nameTempView: String = "PerfStageMetrics"): DataFrame

// Custom aggregations and post-processing of metrics data
// This is legacy and uses Spark DataFrame operations,
// use report instead, which will process data in the driver using Scala
def reportUsingDataFrame(): String

// Shortcut to run and measure the metrics for Spark execution, built after spark.time()
def runAndMeasure[T](f: => T): T

// Helper method to save data, we expect to have small amounts of data so collapsing to 1 partition seems OK
def saveData(df: DataFrame, fileName: String, fileFormat: String = "json", saveMode: String = "default")

/*
 * Send the metrics to Prometheus.
 * serverIPnPort: String with prometheus pushgateway address, format is hostIP:Port,
 * metricsJob: job name,
 * labelName: metrics label name, default is sparkSession.sparkContext.appName,
 * labelValue: metrics label value, default is sparkSession.sparkContext.applicationId
 */
  def sendReportPrometheus(serverIPnPort: String,
                           metricsJob: String,
                           labelName: String = sparkSession.sparkContext.appName,
                           labelValue: String = sparkSession.sparkContext.applicationId): Unit
```

## StageInfoRecorderListener

```
 class StageInfoRecorderListener extends SparkListener

* This listener gathers metrics with Stage execution granularity
* It is based on the Spark Listener interface
* Stage metrics are stored in memory and use to produce a report that aggregates resource consumption
* they can also be consumed "raw" (transformed into a DataFrame and/or saved to a file)

Data structures:

val stageMetricsData: ListBuffer[StageVals] = ListBuffer.empty[StageVals]
val stageIdtoJobId: HashMap[Int, Int] = HashMap.empty[Int, Int]
val stageIdtoJobGroup: HashMap[Int, String] = HashMap.empty[Int, String]
val stageIdtoExecutorMetrics: HashMap[(Int, String), ListBuffer[(String, Long)]]

Methods:

// This methods fires at the end of the stage and collects metrics flattened into the stageMetricsData ListBuffer
override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit
override def onJobStart(jobStart: SparkListenerJobStart): Unit 
// Record executor metrics detailed per stage, this provides memory utilization values. Use with Spark 3.1.0 and above
override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit

// contains the list of task metrics and other measurements of interest at the Stage level,
case class StageVals
```

## StageMetrics - Python

```
StageMetrics class provides the API to collect and process task metrics data aggregated by execution stage.
This is a Python wrapper class to the corresponding Scala class of sparkMeasure.

class StageMetrics:

Example usage:
from sparkmeasure import StageMetrics
stagemetrics = StageMetrics(spark)

stagemetrics.runandmeasure(globals(), 'spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()')
```
Methods: 
```
def begin(self):

def end(self):

def report(self):

def print_report(self):

def report_memory(self):

def print_memory_report(self):

def runandmeasure(self, env, codetorun):

def create_stagemetrics_DF(self, viewname="PerfStageMetrics"):

def save_data(self, df, filepathandname, fileformat="json"):

def remove_listener(self):
```

## TaskMetrics - Scala

```
case class TaskMetrics(sparkSession: SparkSession) 

*  Task Metrics: collects metrics data at Task granularity
*                and provides aggregation and reporting functions for the end-user
*
* Example of how to use task metrics:
* val taskMetrics = ch.cern.sparkmeasure.TaskMetrics(spark)
* taskMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show)
```

Methods:
```
def begin(): Long // Marks the beginning of data collection
def end(): Long  // Marks the end of data collection
def removeListener(): Unit // helper method to remove the listener

// Compute basic aggregation on the Task metrics for the metrics report
// also filter on the time boundaries for the report
def aggregateTaskMetrics() : LinkedHashMap[String, Long] = {

// Custom aggregations and post-processing of metrics data
def report(): String

// Runs report and prints it
def printReport(): Unit

// Legacy transformation of data recorded from the custom Stage listener
// into a DataFrame and register it as a view for querying with SQL
def createTaskMetricsDF(nameTempView: String = "PerfTaskMetrics"): DataFrame = {

// legacy metrics aggregation computed using SQL
def aggregateTaskMetrics(nameTempView: String = "PerfTaskMetrics"): DataFrame

// Custom aggregations and post-processing of metrics data
// This is legacy and uses Spark DataFrame operations,
// use report instead, which will process data in the driver using Scala
def reportUsingDataFrame(): String = {

// Shortcut to run and measure the metrics for Spark execution, built after spark.time()
def runAndMeasure[T](f: => T): T

// Helper method to save data, we expect to have small amounts of data so collapsing to 1 partition seems OK
def saveData(df: DataFrame, fileName: String, fileFormat: String = "json", saveMode: String = "default")

def sendReportPrometheus(serverIPnPort: String,
                           metricsJob: String,
                           labelName: String = sparkSession.sparkContext.appName,
                           labelValue: String = sparkSession.sparkContext.applicationId): Unit
```

##  TaskInfoRecorderListener
```
class TaskInfoRecorderListener() extends SparkListener {

* TaskInfoRecorderListener: this listener gathers metrics with Task execution granularity
* It is based on the Spark Listener interface
* Task metrics are stored in memory and use to produce a report that aggregates resource consumption
* they can also be consumed "raw" (transformed into a DataFrame and/or saved to a file)

Data structures:

val taskMetricsData: ListBuffer[TaskVals] = ListBuffer.empty[TaskVals]
val StageIdtoJobId: collection.mutable.HashMap[Int, Int] = collection.mutable.HashMap.empty[Int, Int]
val StageIdtoJobGroup: collection.mutable.HashMap[Int, String] = collection.mutable.HashMap.empty[Int, String]

Methods:

override def onJobStart(jobStart: SparkListenerJobStart): Unit
override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit

case class TaskVals
* Contains the list of task metrics and other measurements of interest at the Task level, as a case class
```

## TaskMetrics - Python
```
TaskMetrics class provides the API to collect and process task metrics data aggregated by task execution
This is a finer granularity than StageMetrics and potentially collects much more data.
This is a Python wrapper class to the corresponding Scala class of sparkMeasure.

class TaskMetrics:

Example usage:
from sparkmeasure import TaskMetrics

taskmetrics =  TaskMetrics(spark)
taskmetrics.runandmeasure(globals(), 'spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()')
```
Methods:
```
def begin(self):

def end(self):

def report(self):

def print_report(self):

def runandmeasure(self, env, codetorun):

def create_taskmetrics_DF(self, viewname="PerfTaskMetrics"):

def create_taskmetrics_DF(self, viewname="PerfTaskMetrics"):

def create_taskmetrics_DF(self, viewname="PerfTaskMetrics"):

def create_taskmetrics_DF(self, viewname="PerfTaskMetrics"):
```

## Flight Recorder Mode - File Sink
```
class FlightRecorderStageMetrics(conf: SparkConf) extends StageInfoRecorderListener 

class FlightRecorderTaskMetrics(conf: SparkConf) extends TaskInfoRecorderListener

* Use sparkMeasure in flight recording mode to instrument Spark applications without touching their code.
* Flight recorder mode attaches a Spark Listener to the Spark Context, which takes care of
* collecting execution metrics while the application runs and of saving them for later processing.  
* There are two different levels of granularity for metrics collection:
* stage aggregation, with FlightRecorderStageMetrics, and task-level metrics with FlightRecorderTaskMetrics.

* How to use: attach the relevant listener to a Spark Context using the extra listener infrastructure
   For lightRecorderStage:
      --conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics
   For lightRecorderTaskMetrics:  
      --conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderTaskMetrics

Configuration:

--conf spark.sparkmeasure.outputFormat=<format>
    Note: valid values: json,json_to_hadoop,java the default is "json"
--conf spark.sparkmeasure.outputFilename=<output file>
    Note: default = "/tmp/stageMetrics_flightRecorder"
--conf spark.sparkmeasure.printToStdout=<true|false> /
    Note: default is false. Set this to true to print JSON serialized metrics for debug purposes.
```

**Notes:**
  - `json` and `java` serialization formats, write to the driver local filesystem
  - `json_to_hadoop`, writes to JSON serialized metrics to HDFS or to an Hadoop compliant filesystem, such as S3A
  - The amount of data generated by FlightRecorderStageMetrics is relatively small in most applications: O(number_of_stages)
  - FlightRecorderTaskMetrics can generate a large amount of data O(Number_of_tasks), use with care

## InfluxDBSink and InfluxDBSinkExtended

```
class InfluxDBSink(conf: SparkConf) extends SparkListener
class InfluxDBSinkExtended(conf: SparkConf) extends InfluxDBSink(conf: SparkConf) 

* InfluxDBSink: write Spark metrics and application info in near real-time to InfluxDB
*  use this mode to monitor Spark execution workload
*  use for Grafana dashboard and analytics of job execution
*  How to use: attach the InfluxDBSInk to a Spark Context using the extra listener infrastructure.
*   --conf spark.extraListeners=ch.cern.sparkmeasure.InfluxDBSink

* InfluxDBSinkExtended Extends the functionality to record metrics for each executed Task  
* How to use:
*   --conf spark.extraListeners=ch.cern.sparkmeasure.InfluxDBSink,ch.cern.sparkmeasure.InfluxDBSinkExtended

Configuration for InfluxDBSink is handled with Spark configuration parameters:

InfluxDBSink parameters:
spark.sparkmeasure.influxdbURL (default "http://localhost:8086")
spark.sparkmeasure.influxdbUsername (default "")
   Note: username and password can be empty only if InfluxDB runs with auth-enabled=false
spark.sparkmeasure.influxdbPassword (default "")
spark.sparkmeasure.influxdbName (default "sparkmeasure")
     Note: the DB will be created if it does not exist already
spark.sparkmeasure.influxdbStagemetrics, (boolean, default is false)
    Note: when this is true stage metrics will be collected too
spark.sparkmeasure.influxdbEnableBatch, boolean, default true
    Note: this is to improve write performance, 
          but it requires to explicitly stopping Spark Session for clean exit: spark.stop()
          consider setting it to false if this is an issue

This code depends on "influxdb.java", you may need to add the dependency explicitly:
  --packages org.influxdb:influxdb-java:2.14
  Note currently we need to use version 2.14 as newer versions generate jar conflicts (tested up to Spark 3.3.0)
```

## KafkaSink and KafkaSinkExtended

```
class KafkaSink(conf: SparkConf) extends SparkListener
class KafkaSinkExtended(conf: SparkConf) extends KafkaSink(conf) {

**KafkaSink** is a class that extends the SparkListener infrastructure.  
It collects and writes Spark metrics and application info in near real-time to an Apache Kafka backend
provided by the user. Use this mode to monitor Spark execution workload.  
*  How to use: attach the InfluxDBSInk to a Spark Context using the extra listener infrastructure.
*   --conf spark.extraListeners=ch.cern.sparkmeasure.KafkaSink

**KafkaSinkExtended** Extends the functionality to record metrics for each executed Task  
* How to use:
*   --conf spark.extraListeners=ch.cern.sparkmeasure.KafkaSink,ch.cern.sparkmeasure.KafkaSinkExtended


Configuration - KafkaSink parameters:

--conf spark.sparkmeasure.kafkaBroker = Kafka broker endpoint URL
       Example: --conf spark.sparkmeasure.kafkaBroker=kafka.your-site.com:9092
--conf spark.sparkmeasure.kafkaTopic = Kafka topic
       Example: --conf spark.sparkmeasure.kafkaTopic=sparkmeasure-stageinfo
       Note: the topic will be created if it does not yet exist

This code depends on "kafka-clients", you may need to add the dependency explicitly:
  --packages org.apache.kafka:kafka-clients:3.2.1
```

## IOUtils

The object IOUtils contains some helper code for the sparkMeasure package
The methods readSerializedStageMetrics and readSerializedTaskMetrics are used to read data serialized into files by
the "flight recorder" mode.
Two serialization modes are supported currently: java serialization and JSON serialization with jackson library.


```
def writeSerializedJSON(fullPath: String, metricsData: AnyRef): Unit =
def writeSerializedJSONToHadoop(fullPath: String, metricsData: AnyRef, conf: SparkConf): Unit
def writeToStringSerializedJSON(metricsData: AnyRef): String
def writeSerialized(fullPath: String, metricsData: Any): Unit

def readSerializedStageMetricsJSON(stageMetricsFileName: String): List[StageVals]
def readSerializedStageMetrics(stageMetricsFileName: String): ListBuffer[StageVals]
def readSerializedTaskMetricsJSON(taskMetricsFileName: String): List[TaskVals]
def readSerializedTaskMetrics(stageMetricsFileName: String): ListBuffer[TaskVals]
```

## Utils

The object Utils contains some helper code for the sparkMeasure package
The methods formatDuration and formatBytes are used for printing stage metrics reports

```
// Boilerplate code for pretty printing
def formatDuration(milliseconds: Long): String 
def formatBytes(bytes: Long): String
def prettyPrintValues(metric: String, value: Long): String
def encodeTaskLocality(taskLocality: TaskLocality.TaskLocality): Int 

// Return the data structure use to compute metrics reports
def zeroMetricsStage() : LinkedHashMap[String, Long]
def zeroMetricsTask() : LinkedHashMap[String, Long]

// Handle metrics format parameter
def parseMetricsFormat(conf: SparkConf, logger: Logger, defaultFormat:String) : String
def parseMetricsFilename(conf: SparkConf, logger: Logger, defaultFileName:String) : String 

// Parameter parsing
def parsePrintToStdout(conf: SparkConf, logger: Logger, defaultVal:Boolean) : Boolean
def parseMetricsFilename(conf: SparkConf, logger: Logger, defaultFileName:String) : String 
def parseInfluxDBURL(conf: SparkConf, logger: Logger) : String
def parseInfluxDBCredentials(conf: SparkConf, logger: Logger) : (String,String)
def parseInfluxDBName(conf: SparkConf, logger: Logger) : String
def parseInfluxDBStagemetrics(conf: SparkConf, logger: Logger) : Boolean
def parseKafkaConfig(conf: SparkConf, logger: Logger) : (String,String)
```
