# sparkMeasure

[![Build Status](https://travis-ci.org/LucaCanali/sparkMeasure.svg?branch=master)](https://travis-ci.org/LucaCanali/sparkMeasure)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/ch.cern.sparkmeasure/spark-measure_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/ch.cern.sparkmeasure/spark-measure_2.11)

### sparkMeasure is a tool for performance troubleshooting of Apache Spark workloads  
It simplifies the collection and analysis of Spark performance metrics. 
It is also intended as a working example of how to use Spark listeners for collecting and processing 
Spark executors task metrics data.
 * Created and maintained by: Luca.Canali@cern.ch 
   * \+ credits to Viktor.Khristenko@cern.ch + thanks to PR contributors
 * Developed and tested for Spark 2.1.x, 2.2.x, 2.3.x
   * Latest development version: 0.14-SNAPSHOT, last modified August 2018
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

### Documentation and examples

  - **[Scala shell and notebooks](docs/Scala_shell_and_notebooks.md)**
  - **[PySpark and Jupyter notebooks](docs/Python_shell_and_Jupyter.md)**
  - **[Instrument Scala code](docs/Instrument_Scala_code.md)**
  - **[Instrument Python code](docs/Instrument_Python_code.md)**
  - **[Flight Recorder mode](docs/Flight_recorder_mode.md)**
  - [Notes on implementation and APIs](docs/Notes_on_implementation_details.md)
  - [Notes on metrics analysis](docs/Notes_on_metrics_analysis.md)
  - [TODO list and known issues](docs/TODO_and_issues.md)
  
### Architecture diagram  
![sparkMeasure architecture diagram](docs/sparkMeasure_architecture_diagram.png)

### Main concepts underlying sparkMeasure  
* The tool is based on the Spark Listener interface. Listeners transport Spark executor task metrics data from the executor to the driver.
  They are a standard part of Spark instrumentation, used by the Spark Web UI and History Server for example.     
* Metrics can be collected using sparkMeasure at the granularity of stage completion and/or task completion 
 (configurable)
* Metrics are flattened and collected into local memory structures in the driver (ListBuffer of a custom case class).   
* Spark DataFrame and SQL are used to further process metrics data for example to generate reports.  
* Metrics data and reports can be saved for offline analysis.

### FAQ:   
  - Why measuring performance with workload metrics instrumentation rather than just using time?
    - Measuring elapsed time, treats your workload as "a black box" and most often does not allow you
     to understand the root cause of the performance. 
     With workload metrics you can (attempt to) go further in understanding and root cause analysis,
     bottleneck identification, resource usage measurement. 

  - What are Apache Spark tasks metrics and what can I use them for?
     - Apache Spark measures several details of each task execution, including run time, CPU time,
     information on garbage collection time, shuffle metrics and on task I/O. 
     See [taskMetrics class](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/executor/TaskMetrics.scala)

  - How is sparkMeasure different from Web UI/Spark History Server and EventLog?
     - sparkMeasure uses the same ListenerBus infrastructure used to collect data for the Web UI and Spark EventLog.
       - Spark collects metrics and other execution details and exposes them via the Web UI.
       - Notably Task execution metrics are also available through the [REST API](https://spark.apache.org/docs/latest/monitoring.html#rest-api)
       - In addition Spark writes all details of the task execution in the EventLog file 
       (see config of `spark.eventlog.enabled` and `spark.eventLog.dir`)
       - The EventLog is used by the Spark History server + other tools and programs can read and parse
        the EventLog file(s) for workload analysis and performance troubleshooting, see a [proof-of-concept example of reading the EventLog with Spark SQL](https://github.com/LucaCanali/Miscellaneous/blob/master/Spark_Notes/Spark_EventLog.md)  
     - There are key differences that motivate this development: 
        - sparkmeasure can collect data at the stage completion-level, which is more lightweight than measuring
        all the tasks, in case you only need to compute aggregated performance metrics. When needed, 
        sparkMeasure can also collect data at the task granularity level.
        - sparkmeasure has an API that makes it simple to add instrumention/performance measurements
         in notebooks and application code. 
        - sparkmeasure collects data in a flat structure, which makes it natural to use Spark SQL for 
        workload data processing, which provides a simple and powerful interface
        - limitations: sparkMeasure does not collect all the data available in the EventLog, sparkMeasure
        buffers data in the driver memory, [see also the TODO and issues doc](docs/TODO_and_issues.md)

  - What are accumulables?
     - Metrics are first collected into accumulators that are sent from the executors to the driver.
     Many metrics of interest are exposed via [[TaskMetrics]] others are only available in StageInfo/TaskInfo
     accumulables (notably SQL Metrics, such as "scan time")

  - What are known limitations and gotchas?
     - The currently available metrics are quite useful but the do not allow to fully perform 
     workload time profiling and in general do not yet offer a full monitoring platform.
     Metrics are collected on the driver, which can be quickly become a bottleneck.

  - When should I use stage metrics and when should I use task metrics?
     - Use stage metrics whenever possible as they are much more lightweight. Collect metrics at
     the task granularity if you need the extra information, for example if you want to study 
     effects of skew, long tails and task stragglers.

  - How can I save/sink the collected metrics?
     - You can print metrics data and reports to standard output or save them to files (local or on HDFS).
     Additionally you can sink metrics to external systems (such as Prometheus, 
     other sinks like InfluxDB or Kafka may be implemented in future versions). 

  - How can I process metrics data?
     - You can use Spark to read the saved metrics data and perform further post-processing and analysis.
     See the also [Notes on metrics analysis](docs/Notes_on_metrics_analysis.md).

  - How can I contribute to sparkMeasure?
    - SparkMeasure has already profited from PR contributions. Additional contributions are welcome. 
    See the [TODO_and_issues list](docs/TODO_and_issues.md) for a list of known issues and ideas on what 
    you can contribute.

### Two simple examples of sparkMeasure usage
 
1. Link to an [example Python_Jupyter Notebook](examples/SparkMeasure_Jupyer_Python_getting_started.ipynb)

2. An []example Scala notebook on Databricks' platform](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2061385495597958/2729765977711377/442806354506758/latest.html)

3. An example using Scala REPL:
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
  