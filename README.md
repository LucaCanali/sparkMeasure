# Spark Measure 

Spark Measure is a proof-of-concept tool for measuring Apache Spark performance metrics
* Created by Luca.Canali@cern.ch, March 2017
 
It is based on using Spark Listeners as data source and collecting metrics into a ListBuffer of a case class. 
Data is then transformed into a Spark DataFrame for analysis.
 *  Stage Metrics: collects and aggregates metrics at the end of each stage
 *  Task Metrics: collects data at task granularity

Build with sbt and add the target jar to 
<code>spark-submit/spark-shell/pyspark --jars <PATH>/spark-measure_2.11-0.1-SNAPSHOT.jar</code>


It can be use in:
 * Interactive mode from the REPL (spark-shell, pyspark, Jupyter notebook)
 * Inside your code, instrumented to use Spark Measure APIs for collecting data
 * Flight recorder mode: records the performance metrics automatically and saves data for later processing

Supported languages:
 *   The tool is written in Scala, but it can be used both from Scala and Python

Example usage for stage metrics:

Stage metrics:
```val stageMetrics = new ch.cern.sparkmeasure.StageMetrics(spark) 
stageMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show)
```

Task metrics:
```
val taskMetrics = new ch.cern.sparkmeasure.TaskMetrics(spark)
spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
val df = taskMetrics.createTaskMetricsDF()
```

Examples of usage in Python:

Stage metrics:
```
stageMetrics = sc._jvm.ch.cern.sparkmeasure.StageMetrics(spark._jsparkSession)
stageMetrics.begin()
spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
stageMetrics.end()
stageMetrics.printReport()
```

Task Metrics:
```
taskMetrics = sc._jvm.ch.cern.sparkmeasure.TaskMetrics(spark._jsparkSession)
spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
df = taskMetrics.createTaskMetricsDF("PerfTaskMetrics")
spark.sql("select * from PerfTaskMetrics").show()
df.show()
taskMetrics.saveData(df, "taskmetrics_test1", "json")
```

To use in flight recorder mode add:
* for stage metrics: <code>--conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics</code>
* for task metrics: <code>--conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderTaskMetrics</code>

Helper function to deserialize objects saved by the flight mode:
```
val m1 = ch.cern.sparkmeasure.Utils.readSerializedStageMetrics("/tmp/stageMetrics.serialized")
m1.toDF.show
```

Current version 0.1, first release
Developed and tested on Spark 2.1.0
