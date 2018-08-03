## Notes on sparkMeasure implementation details

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
   * def report(): String -> returns a string with the report of the metrics in "PerfStageMetrics" between the timestamps: beginSnapshot and
   endSnapshot
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
   * def report(): String -> returns a string with the report of the metrics in "PerfStageMetrics" between the timestamps: beginSnapshot and
   endSnapshot
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

**Python API:**

See:
 - [stagemetrics.py](python/sparkmeasure/stagemetrics.py)
 - [taskmetrics.py](python/sparkmeasure/taskmetrics.py)
 
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
