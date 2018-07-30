# sparkMeasure

[![Build Status](https://travis-ci.org/LucaCanali/sparkMeasure.svg?branch=master)](https://travis-ci.org/LucaCanali/sparkMeasure)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/ch.cern.sparkmeasure/spark-measure_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/ch.cern.sparkmeasure/spark-measure_2.11)

### SparkMeasure is a tool for performance troubleshooting of Apache Spark workloads  
It simplifies the collection and analysis of Spark performance metrics. 
It is also intended as a working example of how to use Spark listeners for collecting and processing 
Spark executors task metrics data.
 * Created and maintained by: Luca.Canali@cern.ch 
   * \+ credits to Viktor.Khristenko@cern.ch + thanks to PR contributors
 * Developed and tested for Spark 2.1.x, 2.2.x, 2.3.x
   * Latest development version: 0.14-SNAPSHOT, last modified July 2018
 * Build/deploy: 
   - build with `sbt package` or use [sparkMeasure from Maven Central](https://mvnrepository.com/artifact/ch.cern.sparkmeasure)    
   - to install the Python wrapper APIs, `pip install sparkmeasure`
 * Related info:
   - [Link to 2017 blog post on sparkMeasure](http://db-blog.web.cern.ch/blog/luca-canali/2017-03-measuring-apache-spark-workload-metrics-performance-troubleshooting)
   - [Presentation at Spark Summit Europe 2017](https://spark-summit.org/eu-2017/events/apache-spark-performance-troubleshooting-at-scale-challenges-tools-and-methodologies/)  
    
### Use for interactive and batch workloads
 * Interactive: measure and analyze performance from shell or notebooks: using spark-shell (Scala), pyspark (Python) or Jupyter notebooks.
 * Code instrumentation: add calls in your code to deploy sparkMeasure custom Spark listeners and/or use the
 classes StageMetrics/TaskMetrics and related APIs for collecting, analyzing and saving metrics data.
 * "Flight Recorder" mode: this records all performance metrics automatically and saves data for later processing.

### Documentation with examples

  - **[Scala shell and notebooks](docs/Scala_shell_and_notebooks.md)**
  - **[PySpark and Jupyter notebooks](docs/Python_shell_and_Jupyter.md)**
  - **[Instrument Scala code](docs/Instrument_Scala_code.md)**
  - **[Instrument Python code](docs/Instrument_Python_code.md)**
  - Flight Recorder mode

### Architecture diagram  
[TODO]

### Main concepts underlying sparkMeasure  
* The tool is based on the Spark Listener interface. Listeners transport Spark executor task metrics data from the executor to the driver.
  They are a standard part of Spark instrumentation, used by the Spark Web UI for example.     
* Metrics can be collected using sparkMeasure at the granularity of stage completion and/or task completion 
 (configurable)
* Metrics are flattened and collected into local memory structures in the driver (ListBuffer of a custom case class).   
* Data is then transformed into a Spark DataFrame for analysis.  
* Data can be saved for offline analysis

### FAQ:   
  - Why measuring performance with workload metrics instrumentation rather than just using time?
    - Measuring elapsed time of your job gives you a measure related to the specific execution of your job.
      With workload metrics you can (attempt to) go further in understanding the behavior of your job:
      as in doing root cause analysis, bottleneck identification, resource usage measurement 
  - What are Apache Spark tasks metrics and what can I use them for?
  - What are accumulables?
  - What are known limitations and gotchas?
  - When should I use stage metrics and when should I use task metrics?
  - How can I save/sink the collected metrics?
  - How can I process metrics data?
  - How can I contribute to sparkMeasure?
    - See the [TODO_and_issues list](docs/TODO_and_issues.md), send PRs or open issues on Github.

### A simple example of sparkMeasure usage
 
1. Measure metrics at the Stage level (example in Scala):
```
bin/spark-shell --packages ch.cern.sparkmeasure:spark-measure_2.11:0.13

val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark) 
stageMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show())
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
  