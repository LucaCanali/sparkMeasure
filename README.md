# sparkMeasure

sparkMeasure is a custom tool for performance investigations in Spark and provides proof-of-concept code for 
collecting and analyzing workload metrics.
 * Created by Luca.Canali@cern.ch, March 2017
 * Additional credits to: Viktor Khristenko 
 * Originally developed and tested with Spark 2.1.0 
 * Current version 0.1, first release

Main ideas of how it sparkMeasure works:
The tool is based on the Spark Listener interface, that is used as the data source. 
Metrics and flattened and collected into a ListBuffer of a case class. 
Data is then transformed into a Spark DataFrame for analysis.

**How to build** use sbt and add the target jar to 
<code>spark-submit/spark-shell/pyspark --jars <PATH>/spark-measure_2.11-0.1-SNAPSHOT.jar</code>

sparkMeasure can be used:
 * To measure and analyze performance on the REPL: spark-shell (Scala), pyspark (Python) or Jupyter notebooks
 * Inside your code, instrumented to use Spark Measure APIs for collecting data
 * For batch jobs using the Flight Recorder mode: this records the performance metrics automatically and saves data for later processing

Examples
 
Stage metrics, Scala:
```
val stageMetrics = new ch.cern.sparkmeasure.StageMetrics(spark) 
stageMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show)
```

Stage metrics, alternative way to collect and print metrics, Scala:
```
val stageMetrics = new ch.cern.sparkmeasure.StageMetrics(spark) 
stageMetrics.begin()

...execute one or more Spark jobs...

stageMetrics.end()
stageMetrics.printReport()
```

Print additional accumulables metrics collected at stage-level, Scala:
```
stageMetrics.printAccumulables()
```

Task metrics, Scala:
```
val taskMetrics = new ch.cern.sparkmeasure.TaskMetrics(spark)
spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
val df = taskMetrics.createTaskMetricsDF()
```

Stage metrics, Python:
```
stageMetrics = sc._jvm.ch.cern.sparkmeasure.StageMetrics(spark._jsparkSession)
stageMetrics.begin()
spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
stageMetrics.end()
stageMetrics.printReport()
```

Task Metrics, Python:
```
taskMetrics = sc._jvm.ch.cern.sparkmeasure.TaskMetrics(spark._jsparkSession)
spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
df = taskMetrics.createTaskMetricsDF("PerfTaskMetrics")
spark.sql("select * from PerfTaskMetrics").show()
df.show()
taskMetrics.saveData(df, "taskmetrics_test1", "json")
```

Flight Recorder mode add:
* for stage metrics: <code>--conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics</code>
* for task metrics: <code>--conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderTaskMetrics</code>

Helper function to deserialize objects saved by the flight mode:
```
val m1 = ch.cern.sparkmeasure.Utils.readSerializedStageMetrics("/tmp/stageMetrics.serialized")
m1.toDF.show
```

---
**Additional info on Stage Metrics:**

* class StageInfoRecorderListener extends SparkListener -> collects metrics at the end of each Stage
* case class StageVals -> used to collect and store "flatten" the stageinfo and TaskMetric info 
  collected by the Listener. Metrics are aggregated per stage and include: executor run time, 
  CPU time, shuffle read and write time, serialization and deserialization time, HDFS I/O metrics, etc
* case class accumulablesInfo -> used to collect and store the metrics of type "accumulables"

* case class StageMetrics(sparkSession: SparkSession)-> instantiate this class to start measuring Stage metrics
   * Metrics are collected in a ListBuffer of case class StageVals for metrics generating from TaskMetrics and in a ListBuffer of accumulablesInfo
   for metrics generated from "accumulables"
   * def begin() and def end() methods -> use them at mark beginning and end of data collection if you plan to use printReport()
   * def createStageMetricsDF(nameTempView: String = "PerfStageMetrics"): DataFrame -> converts the ListBuffer with stage 
   metrics into a DataFrame and creates a temporary view, useful for data analytics
   * def createAccumulablesDF(nameTempView: String = "AccumulablesMetrics"): DataFrame -> converts the accumulables agrgegate
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

* class TaskInfoRecorderListener extends SparkListener > collects metrics at the end of each Task
* case class TaskVals -> used to collect and store "flatten" TaskMetric info collected by the Listener.
Metrics are collected per task and include:executor run time,  CPU time, scheduler delay, shuffle read and write time, 
serialization and deserialization time, HDFS I/O metrics, etc 
  read and write time, serializa and deserialization time, HDFS I/O metrics, etc
* case class TaskMetrics(sparkSession: SparkSession -> instantiate this class to start measuring Task metrics
   * def createTaskMetricsDF(nameTempView: String = "PerfTaskMetrics"): DataFrame ->  converts the ListBuffer with stage 
     metrics into a DataFrame and creates a temporary view, useful for data analytics
   * def saveData(df: DataFrame, fileName: String, fileFormat: String = "json") -> helper method to save metrics data collected 
      in a DataFrame for later analysis/plotting

**Additional info on Flight Recorder Mode:**

To use in flight recorder mode add one or both of the following to the spark-submit/spark-shell/pyspark command line:
 * --conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics
 * --conf class FlightRecorderTaskMetrics(conf: SparkConf) extends TaskInfoRecorderListener

The flight recorder mode writes the collected metrics serializaed into a file in the driver's filesystem. 
Optionally add one or both of the following configuration parameters to determine the path of the output file  
--conf spark.executorEnv.stageMetricsFileName"=<file path> (default is "/tmp/stageMetrics.serialized")
--conf spark.executorEnv.taskMetricsFileName"=<file path> (default is "/tmp/taskMetrics.serialized")
 
**Additional info on Utils:**

The object Utils contains some helper code for the sparkMeasure package
 * The methods formatDuration and formatBytes are used for printing stage metrics reports
 * The methods readSerializedStageMetrics and readSerializedTaskMetrics are used to read data serialized 
 into files by "flight recorder" mode

Examples:
```
val taskVals = ch.cern.sparkmeasure.Utils.readSerializedTaskMetrics("<file name>")
val taskMetricsDF = taskVals.toDF

val stageVals = ch.cern.sparkmeasure.Utils.readSerializedStageMetrics("<file name>")
val stageMetricsDF = stageVals.toDF
```