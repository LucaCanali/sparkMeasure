# sparkMeasure

[![Build Status](https://travis-ci.org/LucaCanali/sparkMeasure.svg?branch=master)](https://travis-ci.org/LucaCanali/sparkMeasure)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/ch.cern.sparkmeasure/spark-measure_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/ch.cern.sparkmeasure/spark-measure_2.11)

**sparkMeasure is a tool for performance troubleshooting of Apache Spark workloads**  
It simplifies the collection and analysis of Spark performance metrics.  
It is also intended also as proof-of-concept code on how to use Spark Listeners for custom Spark metrics collection. 
 * Created and maintained by: Luca.Canali@cern.ch + additional credits to: Viktor.Khristenko@cern.ch 
 * Developed and tested for Spark 2.1.x and 2.2.x
 * Build with `sbt package`
   - Latest development version 0.12-SNAPSHOT, last modified November 2017
   - sparkMeasure on Maven Central: [https://mvnrepository.com/artifact/ch.cern.sparkmeasure]    
 * Related info:
   - [Link to a blog post on sparkMeasure](http://db-blog.web.cern.ch/blog/luca-canali/2017-03-measuring-apache-spark-workload-metrics-performance-troubleshooting)
   - [Get started note](https://github.com/LucaCanali/Miscellaneous/blob/master/Spark_Notes/Spark_Performace_Tool_sparkMeasure.md)
   - [Presentation at Spark Summit Europe 2017](https://spark-summit.org/eu-2017/events/apache-spark-performance-troubleshooting-at-scale-challenges-tools-and-methodologies/)  
    
**Use sparkMeasure for:**
 * Performance troubleshooting: measure and analyze performance interactively from spark-shell (Scala), pyspark (Python) or Jupyter notebooks.
 * Code instrumentation: add calls in your code to deploy sparkMeasure custom Listeners and/or use the
 classes StageMetrics/TaskMetrics and related APIs for collecting, analyzing and optionally saving metrics data.
 * Measure workloads that you cannot change: use sparkMeasure in the "Flight Recorder" mode, this records the performance metrics automatically and saves data for later processing.

**Main concepts underlying sparkMeasure:**  
* The tool is based on the Spark Listener interface. Listeners transport Spark executor task metrics data from the executor to the driver.
  They are a standard part of Spark instrumentation, used by the Spark Web UI for example.     
* Metrics can be collected using sparkMeasure at the granularity of stage complettion and/or task completion 
 (configurable by the tool user)
* Metrics are flattened and collected into local memory structures (ListBuffer of a case class).   
* Data is then transformed into a Spark DataFrame for analysis.  
* Data can be saved for offline analysis

**How to use:** use sbt to package the jar from source, or use the jar available on Maven Central. Example:     
```scala
bin/spark-shell --packages ch.cern.sparkmeasure:spark-measure_2.11:0.11
```
or use the jar as in :
```scala
spark-submit/pyspark/spark-shell --jars spark-measure_2.11-0.12-SNAPSHOT.jar
```

**Examples**
 
1. Measure metrics at the Stage level (example in Scala):
```
bin/spark-shell --packages ch.cern.sparkmeasure:spark-measure_2.11:0.11

val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark) 
stageMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show)
```


The output should look like this:
```
Scheduling mode = FIFO
Spark Context default degree of parallelism = 8
Aggregated Spark stage metrics:
numStages => 3
sum(numTasks) => 17
elapsedTime => 9103 (9 s)
sum(stageDuration) => 9027 (9 s)
sum(executorRunTime) => 69238 (1.2 min)
sum(executorCpuTime) => 68004 (1.1 min)
sum(executorDeserializeTime) => 1031 (1 s)
sum(executorDeserializeCpuTime) => 151 (0.2 s)
sum(resultSerializationTime) => 5 (5 ms)
sum(jvmGCTime) => 64 (64 ms)
sum(shuffleFetchWaitTime) => 0 (0 ms)
sum(shuffleWriteTime) => 26 (26 ms)
max(resultSize) => 17934 (17.0 KB)
sum(numUpdatedBlockStatuses) => 0
sum(diskBytesSpilled) => 0 (0 Bytes)
sum(memoryBytesSpilled) => 0 (0 Bytes)
max(peakExecutionMemory) => 0
sum(recordsRead) => 2000
sum(bytesRead) => 0 (0 Bytes)
sum(recordsWritten) => 0
sum(bytesWritten) => 0 (0 Bytes)
sum(shuffleTotalBytesRead) => 472 (472 Bytes)
sum(shuffleTotalBlocksFetched) => 8
sum(shuffleLocalBlocksFetched) => 8
sum(shuffleRemoteBlocksFetched) => 0
sum(shuffleBytesWritten) => 472 (472 Bytes)
sum(shuffleRecordsWritten) => 8
```


2. This is an alternative way to collect and print metrics (Scala):
```scala
val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark) 
stageMetrics.begin()

...execute one or more Spark jobs...

stageMetrics.end()
stageMetrics.printReport()
stageMetrics.printAccumulables
```

3. Print additional accumulables metrics (including SQL metrics) collected at stage-level, Scala:
```scala
stageMetrics.printAccumulables()
```

4. Collect and report Task metrics, Scala:
```scala
val taskMetrics = ch.cern.sparkmeasure.TaskMetrics(spark)
taskMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show)
```

5. How to collect stage metrics, example in Python:
```python
stageMetrics = sc._jvm.ch.cern.sparkmeasure.StageMetrics(spark._jsparkSession)
stageMetrics.begin()
spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
stageMetrics.end()
stageMetrics.printReport()
stageMetrics.printAccumulables()
```

6. How to collect task metrics, example in Python: 
```python
taskMetrics = sc._jvm.ch.cern.sparkmeasure.TaskMetrics(spark._jsparkSession, False)
taskMetrics.begin()
spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
taskMetrics.end()
taskMetrics.printReport()

# As an alternative to using begin() and end(), you can run the following:
df = taskMetrics.createTaskMetricsDF("PerfTaskMetrics")
spark.sql("select * from PerfTaskMetrics").show()
df.show()
taskMetrics.saveData(df, "taskmetrics_test1", "json")
```

**Flight Recorder mode**
This is for instrumenting Spark applications without touching their code. Just add an extra custom listener that will 
record the metrics of interest and save to a file at the end of the application.
* For recording stage metrics: <code>--conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics</code>
* For recording task-level metrics: <code>--conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderTaskMetrics</code>

To post-process the saved metrics you will need to deserialize objects saved by the flight mode. This is an example of
how to do that using the supplied helper object sparkmeasure.Utils
```scala
val m1 = ch.cern.sparkmeasure.Utils.readSerializedStageMetrics("/tmp/stageMetrics.serialized")
m1.toDF.show
```

**Analysis of performance metrics:**  
One of the key features of sparkMeasure is that it makes data easily accessible for analysis.  
This is achieved by exporting the collected data into Spark DataFrames where they can be queries with Spark APIs and/or SQL.
In addition the metrics can be used for plotting and other visualizations, for example using Jupyter notebooks.

Example of analysis of Task Metrics using a Jupyter notebook at: [SparkTaskMetricsAnalysisExample.ipynb](examples/SparkTaskMetricsAnalysisExample.ipynb)

Additional example code:
```scala
// export task metrics collected by the Listener into a DataFrame and registers as a temporary view 
val df = taskMetrics.createTaskMetricsDF("PerfTaskMetrics")

// other option: read metrics previously saved on a json file
val df = spark.read.json("taskmetrics_test1")
df.createOrReplaceTempView("PerfTaskMetrics")

// show the top 5 tasks by duration
spark.sql("select jobId, host, duration from PerfTaskMetrics order by duration desc limit 5").show()
// show the available metrics
spark.sql("desc PerfTaskMetrics").show()
```
   

---
**Additional info on Stage Metrics implementation:**

* class StageInfoRecorderListener extends SparkListener
   * Collects metrics at the end of each Stage
   * This is the main engine to collect metrics. Metrics are collected in a ListBuffer of case class StageVals for metrics generating from TaskMetrics and in a ListBuffer of accumulablesInfo
   for metrics generated from "accumulables".
* case class StageVals -> used to collect and store "flatten" the stageinfo and TaskMetric info 
  collected by the Listener. Metrics are aggregated per stage and include: executor run time, 
  CPU time, shuffle read and write time, serialization and deserialization time, HDFS I/O metrics, etc
* case class accumulablesInfo -> used to collect and store the metrics of type "accumulables"

* case class StageMetrics(sparkSession: SparkSession)
   * Helper class to help in collecting and storing performance metrics. It provides wrapper methods to add the listener to
   the Spark Context (ListenerBus) and other other methods for analysis. When you instantiate this class you start collecting 
   stage metrics data.
   * def begin() and def end() methods -> use them at mark beginning and end of data collection if you plan to use printReport()
   * def createStageMetricsDF(nameTempView: String = "PerfStageMetrics"): DataFrame -> converts the ListBuffer with stage 
   metrics into a DataFrame and creates a temporary view, useful for data analytics
   * def createAccumulablesDF(nameTempView: String = "AccumulablesStageMetrics"): DataFrame -> converts the accumulables aggregated
   at stage level in a ListBuffer into a DataFrame and temporary view
   * def printReport(): Unit -> prints a report of the metrics in "PerfStageMetrics" between the timestamps: beginSnapshot and
   endSnapshot
   * def printAccumulables(): Unit -> prints the accumulables metrics divided in 2 groups: internal metrics (which are
   basically the same as TaskMetrics) and the rest (typically metrics generated custom by parts of the SQL execution engine)
   * def runAndMeasure[T](f: => T): T -> a handy extension to do 3 actions: runs the Spark workload, measure its metrics
   and print the report. You can see this as an extension of spark.time() command
   * def saveData(df: DataFrame, fileName: String, fileFormat: String = "json") -> helper method to save metrics data collected 
   in a DataFrame for later analysis/plotting
   
   
**Additional info on Task Metrics:**

* case class TaskMetrics(sparkSession: SparkSession, gatherAccumulables: Boolean = false)
   * Collects metrics at the end of each Task
   * This is the main engine to collect metrics. Metrics are collected in a ListBuffer of case class TaskVals
   * optionally gathers accumulabels (with task metrics and SQL metrics per task if gatherAccumulables is set to true)
* case class TaskVals -> used to collect and store "flatten" TaskMetric info collected by the Listener.
Metrics are collected per task and include:executor run time,  CPU time, scheduler delay, shuffle read and write time, 
serialization and deserialization time, HDFS I/O metrics, etc 
  read and write time, serializa and deserialization time, HDFS I/O metrics, etc
* case class TaskMetrics(sparkSession: SparkSession
   * Helper class to help in collecting and storing performance metrics. It provides wrapper methods to add the listener to
      the Spark Context (ListenerBus) and other other methods for analysis. When you instantiate this class you start collecting 
      task-level metrics data.
   * def begin() and def end() methods -> use them at mark beginning and end of data collection if you plan to use printReport()
   * def printReport(): Unit -> prints a report of the metrics in "TaskStageMetrics" between the timestamps: beginSnapshot and
   endSnapshot
   * def createTaskMetricsDF(nameTempView: String = "PerfTaskMetrics"): DataFrame ->  converts the ListBuffer with stage 
     metrics into a DataFrame and creates a temporary view, useful for data analytics
   * def runAndMeasure[T](f: => T): T -> a handy extension to do 3 actions: runs the Spark workload, measure its metrics
   and print the report. You can see this as an extension of spark.time() command     
   * def saveData(df: DataFrame, fileName: String, fileFormat: String = "json") -> helper method to save metrics data collected 
      in a DataFrame for later analysis/plotting
   * def createAccumulablesDF(nameTempView: String = "AccumulablesTaskMetrics"): DataFrame -> converts the accumulables aggregated
   at task level in a ListBuffer into a DataFrame and temporary view
   * def printAccumulables(): Unit -> prints the accumulables metrics divided in 2 groups: internal metrics (which are
      basically the same as TaskMetrics) and the rest (typically metrics generated custom by parts of the SQL execution engine)


**Additional info on Flight Recorder Mode:**

To use in flight recorder mode add one or both of the following to the spark-submit/spark-shell/pyspark command line:
* For recording stage metrics: <code>--conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics</code>
* For recording task-level metrics: <code>--conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderTaskMetrics</code>

The flight recorder mode writes the collected metrics serialized into a file in the driver's filesystem. 
Optionally add one or both of the following configuration parameters to determine the path of the output file  
 * `--conf spark.executorEnv.stageMetricsFileName=<file path>` (the default is "/tmp/stageMetrics.serialized")
 * `--conf spark.executorEnv.taskMetricsFileName=<file path>` (the default is "/tmp/taskMetrics.serialized")
 
Optionally use JSON to serialize the metrics collected in Flight Recorded mode:
 * --conf `--conf spark.executorEnv.taskMetricsFormat="json"` (the default is "java")
 
**Additional info on Utils:**

The object Utils contains some helper code for the sparkMeasure package
 * The methods formatDuration and formatBytes are used for printing stage metrics reports
 * The methods readSerializedStageMetrics and readSerializedTaskMetrics are used to read data serialized 
 into files by "flight recorder" mode

Examples:
```scala
val taskVals = ch.cern.sparkmeasure.Utils.readSerializedTaskMetrics("<file name>")
val taskMetricsDF = taskVals.toDF

val stageVals = ch.cern.sparkmeasure.Utils.readSerializedStageMetrics("<file name>")
val stageMetricsDF = stageVals.toDF
```

**Known issues and TODO list**
   * gatherAccumulables=true for taskMetrics(sparkSession: SparkSession, gatherAccumulables: Boolean) 
   currently only works only on Spark 2.1.x and breaks from Spark 2.2.1. This is a consequence of
   [SPARK PR 17596](https://github.com/apache/spark/pull/17596).
   Todo: restore the functionality of measuring task accumulables for Spark 2.2.x.
   * Task/stage failures and other errors are mostly not handled by the code in this version, this puts the effort
   on the user to validate the output. This needs to be fixed in a future version.
   * Following [SPARK PR 18249](https://github.com/apache/spark/pull/18249/files) add support for the newly introduced 
   remoteBytesReadToDisk Task Metric (I believe this is for Spark 2.3, to be checked).
   * Following [SPARK PR 18162](https://github.com/apache/spark/pull/18162) TaskMetrics._updatedBlockStatuses is off by 
   default, so maybe can be taken out of the list of metrics collected by sparkMetric.
   
