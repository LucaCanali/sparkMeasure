# sparkMeasure

[![Build Status](https://travis-ci.org/LucaCanali/sparkMeasure.svg?branch=master)](https://travis-ci.org/LucaCanali/sparkMeasure)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/ch.cern.sparkmeasure/spark-measure_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/ch.cern.sparkmeasure/spark-measure_2.11)

**SparkMeasure is a tool for performance troubleshooting of Apache Spark workloads**  
It simplifies the collection and analysis of Spark performance metrics.  
It is also intended as a working example of how to use Spark listeners for collecting and processing 
Spark executors task metrics data.
 * Created and maintained by: Luca.Canali@cern.ch 
   * credits for work on original prototype to Viktor.Khristenko@cern.ch
   * thanks to contributors who have submitted PRs 
 * Developed and tested for Spark 2.1.x, 2.2.x, 2.3.x
 * Usage: 
   - build with `sbt package`. Latest development version 0.12-SNAPSHOT, last modified July 2018
   - or use sparkMeasure from Maven Central: [https://mvnrepository.com/artifact/ch.cern.sparkmeasure]    
 * Related info:
   - [Link to a blog post on sparkMeasure](http://db-blog.web.cern.ch/blog/luca-canali/2017-03-measuring-apache-spark-workload-metrics-performance-troubleshooting)
   - [Get started note](https://github.com/LucaCanali/Miscellaneous/blob/master/Spark_Notes/Spark_Performace_Tool_sparkMeasure.md)
   - [Presentation at Spark Summit Europe 2017](https://spark-summit.org/eu-2017/events/apache-spark-performance-troubleshooting-at-scale-challenges-tools-and-methodologies/)  
    
**Multiple usage models**
 * Interactive: measure and analyze performance from shell or notebooks: using spark-shell (Scala), pyspark (Python) or Jupyter notebooks.
 * Code instrumentation: add calls in your code to deploy sparkMeasure custom Spark listeners and/or use the
 classes StageMetrics/TaskMetrics and related APIs for collecting, analyzing and saving metrics data.
 * "Flight Recorder" mode: this records all performance metrics automatically and saves data for later processing.

**Docs:**
  - Scala shell and notebooks
  - Python shell and Jupyter notebooks
  - Instrument Scala code
  - Instrument Python code
  - Flight Recorder mode

**Main concepts underlying sparkMeasure**  
* The tool is based on the Spark Listener interface. Listeners transport Spark executor task metrics data from the executor to the driver.
  They are a standard part of Spark instrumentation, used by the Spark Web UI for example.     
* Metrics can be collected using sparkMeasure at the granularity of stage completion and/or task completion 
 (configurable)
* Metrics are flattened and collected into local memory structures in the driver (ListBuffer of a custom case class).   
* Data is then transformed into a Spark DataFrame for analysis.  
* Data can be saved for offline analysis

**Architecture diagram**  
[TODO]

**FAQ:**   
  - Why should I care about instrumentation?
  - What are Sparks tasks metrics and what can I use them for?
  - What are accumulables?
  - What are known limitations and gotchas?
  - When should I use stage metrics and when whould I use task metrics?
  - Where can I save/sink the collected metrics?
  - How can I process metrics data?
  - How to I contribute to sparkMeasure?

**How to use:** use sbt to package the jar from source, or use the jar available on Maven Central. Example:     
```scala
bin/spark-shell --packages ch.cern.sparkmeasure:spark-measure_2.11:0.11
```
or just use the jar (it is only needed in the driver) as in:
```scala
spark-submit/pyspark/spark-shell --conf spark.driver.extraClassPath=<path>/spark-measure_2.11-0.12-SNAPSHOT.jar
```

**TLDR; one basic example of sparkMeasure usage**
 
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
  