# sparkMeasure

**sparkMeasure is a tool for performance investigations of Apache Spark workloads.**  
It simplifies the collection and analysis of Spark performance metrics.
It is intended also as proof-of-concept code on how to use Spark listeners for custom metrics collection. 
 * Created by Luca.Canali@cern.ch, March 2017
 * Additional credits: Viktor Khristenko 
 * Version 0.1 beta, last modified May 2017
    * developed and tested on Spark 2.1.0 and 2.1.1
    * note and todo: TaskMetrics implementation need work (for Spark 2.2 and higher) to comply with the changes in from [SPARK-12837](https://github.com/apache/spark/pull/17596) 
 * [Link to the accompanying blog post](http://db-blog.web.cern.ch/blog/luca-canali/2017-03-measuring-apache-spark-workload-metrics-performance-troubleshooting)

**Main ideas of how sparkMeasure works:**  
* The tool is based on the Spark Listener interface, that is used as source for Spark workload metrics data.     
* Metrics are collected at the granularity or stage and task (configurable)
* Metrics are flattened and collected into a ListBuffer of a case class.   
* Data is then transformed into a Spark DataFrame for analysis.  
* Data can be saved for offline analysis

**Where sparkMeasure can be useful:**
 * REPL: To measure and analyze performance interactively from: spark-shell (Scala), pyspark (Python) or Jupyter notebooks
 * Inside your code: add instrumentation calls in your code to use sparkMeasure custom Listeners and/or use the
 classes StageMetrics/TaskMetrics and related APIs for collecting, analyzing and optionally saving metrics data
 * Instrument code that you cannot change: use sparkMeasure in the "Flight Recorde"r mode, this records the performance metrics automatically and saves data for later processing

**How to use:** use sbt to package (or use the jar uploaded in the target/scala-2.11 folder if relevant to your environemnt).  
Run by adding the target jar to 
<code>spark-submit/spark-shell/pyspark --jars <PATH>/spark-measure_2.11-0.1-SNAPSHOT.jar</code>

**Examples**
 
1. Measure metrics at the Stage level (example in Scala):
```scala
val stageMetrics = new ch.cern.sparkmeasure.StageMetrics(spark) 
stageMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show)
```

2. This is an alternative way to collect and print metrics (Scala):
```scala
val stageMetrics = new ch.cern.sparkmeasure.StageMetrics(spark) 
stageMetrics.begin()

...execute one or more Spark jobs...

stageMetrics.end()
stageMetrics.printReport()
```

3. Print additional accumulables metrics collected at stage-level, Scala:
```scala
stageMetrics.printAccumulables()
```

4. Collect and report Task metrics, Scala:
```scala
val taskMetrics = new ch.cern.sparkmeasure.TaskMetrics(spark)
taskMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show)
```

5. Stage metrics example in Python:
```python
stageMetrics = sc._jvm.ch.cern.sparkmeasure.StageMetrics(spark._jsparkSession)
stageMetrics.begin()
spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
stageMetrics.end()
stageMetrics.printReport()
```

6. Task metrics example in Python: 
```python
taskMetrics = sc._jvm.ch.cern.sparkmeasure.TaskMetrics(spark._jsparkSession)
taskMetrics.begin()
spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
taskMetrics.end()
taskMetrics.printReport()
// alternatively:
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

* class TaskInfoRecorderListener extends SparkListener
   * Collects metrics at the end of each Task
   * This is the main engine to collect metrics. Metrics are collected in a ListBuffer of case class TaskVals
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
 * --conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics
 * --conf class FlightRecorderTaskMetrics(conf: SparkConf) extends TaskInfoRecorderListener

The flight recorder mode writes the collected metrics serializaed into a file in the driver's filesystem. 
Optionally add one or both of the following configuration parameters to determine the path of the output file  
 * --conf spark.executorEnv.stageMetricsFileName"=<file path> (default is "/tmp/stageMetrics.serialized")
 * --conf spark.executorEnv.taskMetricsFileName"=<file path> (default is "/tmp/taskMetrics.serialized")
 
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