# SparkMeasure - a performance tool for Apache Spark

[![Test](https://github.com/LucaCanali/sparkmeasure/actions/workflows/build_with_scala_and_python_tests.yml/badge.svg)](https://github.com/LucaCanali/sparkmeasure/actions/workflows/build_with_scala_and_python_tests.yml)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/ch.cern.sparkmeasure/spark-measure_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/ch.cern.sparkmeasure/spark-measure_2.12)
[![DOI](https://zenodo.org/badge/85240663.svg)](https://doi.org/10.5281/zenodo.7891017)
[![PyPI](https://img.shields.io/pypi/v/sparkmeasure.svg)](https://pypi.org/project/sparkmeasure/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/sparkmeasure)](https://pypistats.org/packages/sparkmeasure)
[![API Documentation](https://img.shields.io/badge/API-Documentation-brightgreen)](docs/Reference_SparkMeasure_API_and_Configs.md)

SparkMeasure is a tool and a library designed to ease performance measurement and troubleshooting of
Apache Spark jobs. It focuses on easing the collection and analysis of Spark metrics,
making it a practical choice for both developers and data engineers.
With sparkMeasure, users gain a deeper understanding of their Spark job performance,
enabling faster and more reliable data processing workflows.

### ✨ Highlights
- **Interactive Troubleshooting:** Ideal for real-time analysis of Spark workloads in notebooks
and spark-shell/pyspark environments.
- **Development & CI/CD Integration:** Facilitates testing, measuring, and comparing execution metrics
  of Spark jobs under various configurations or code changes.
- **Batch Job Analysis:** With Flight Recorder mode sparkMeasure transparently records batch job metrics
  for later analysis.
- **Monitoring Capabilities:** Integrates with external systems like Apache Kafka, 
  Prometheus Push Gateway, Prometheus JMX Exporter, and InfluxDB for extensive monitoring.
- **Educational Tool:** Serves as a practical example of implementing Spark Listeners for the collection
  of detailed Spark task metrics.
- **Language Compatibility:** Fully supports Scala, Java, and Python, making it versatile for a wide range
  of Spark applications.

### 📚 Table of Contents
- [SparkMeasure - a performance tool for Apache Spark](#sparkmeasure---a-performance-tool-for-apache-spark)
    - [✨ Highlights](#highlights)
    - [📚 Table of Contents](#tableofcontents)
    - [Links to related work on Spark Performance](#links-to-related-work-on-spark-performance)
  - [🚀 Quick start](#quickstart)
    - [Examples of sparkMeasure on notebooks](#examples-of-sparkmeasure-on-notebooks)
    - [Examples of sparkMeasure on the CLI](#examples-of-sparkmeasure-on-the-cli)
      - [Python CLI](#python-cli)
      - [Scala CLI](#scala-cli)
    - [Memory report](#memory-report)
    - [CLI example for Task Metrics:](#cli-example-for-task-metrics)
  - [Setting Up SparkMeasure with Spark](#setting-up-sparkmeasure-with-spark)
    - [Version Compatibility for SparkMeasure](#version-compatibility-for-sparkmeasure)
    - [📥 Downloading SparkMeasure](#-downloading-sparkmeasure)
    - [Setup Examples](#setup-examples)
      - [Spark 4 with Scala 2.13](#spark-4-with-scala-213)
      - [Spark 3 with Scala 2.12](#spark-3-with-scala-212)
    - [Including sparkMeasure in your Spark environment](#including-sparkmeasure-in-your-spark-environment)
  - [Running unit tests](#running-unit-tests)
  - [Notes on Spark Metrics](#notes-on-spark-metrics)
  - [Documentation, API, and examples](#documentation-api-and-examples)
  - [Architecture diagram](#architecture-diagram)
  - [Main concepts underlying sparkMeasure implementation](#main-concepts-underlying-sparkmeasure-implementation)
    - [FAQ:](#faq)

### Links to related work on Spark Performance

- **[Building an Apache Spark Performance Lab](https://db-blog.web.cern.ch/node/195)**
  Guide to setting up a Spark performance testing environment.
- **[TPC-DS Benchmark with PySpark](https://github.com/LucaCanali/Miscellaneous/tree/master/Performance_Testing/TPCDS_PySpark)**
  Tool for running TPC-DS with PySpark, instrumented with `sparkMeasure`.
- **[Spark Monitoring Dashboard](https://github.com/cerndb/spark-dashboard)**
  Custom monitoring solution with real-time dashboards for Spark.
- **[Flamegraphs for Profiling Spark Jobs](https://github.com/LucaCanali/Miscellaneous/blob/master/Spark_Notes/Tools_Spark_Pyroscope_FlameGraph.md)**
  Guide to profiling Spark with Pyroscope and Flamegraphs.
- **[Advanced Notes on Apache Spark](https://github.com/LucaCanali/Miscellaneous/tree/master/Spark_Notes)**
  Tips, configuration, and troubleshooting for Spark.
- **[Introductory Course on Apache Spark](https://sparktraining.web.cern.ch/)**
  Beginner-friendly course on Spark fundamentals.

Main author and contact: Luca.Canali@cern.ch

---
## 🚀 Quick start

[![Watch the video](https://www.youtube.com/s/desktop/050e6796/img/favicon_32x32.png) Watch sparkMeasure's getting started demo tutorial](https://www.youtube.com/watch?v=NEA1kkFcZWs)

### Examples of sparkMeasure on notebooks
- Run locally or on hosted resources like Google Colab, Databricks, GitHub Codespaces, etc on Jupyter notebooks

- [<img src="https://raw.githubusercontent.com/googlecolab/open_in_colab/master/images/icon128.png" height="50"> Jupyter notebook on Google Colab Research](https://colab.research.google.com/github/LucaCanali/sparkMeasure/blob/master/examples/SparkMeasure_Jupyter_Colab_Example.ipynb)

- [<img src="https://upload.wikimedia.org/wikipedia/commons/6/63/Databricks_Logo.png" height="40"> Scala notebook on Databricks](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2061385495597958/2910895789597333/442806354506758/latest.html)

- [<img src="https://upload.wikimedia.org/wikipedia/commons/6/63/Databricks_Logo.png" height="40"> Python notebook on Databricks](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2061385495597958/2910895789597316/442806354506758/latest.html)

- [<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/3/38/Jupyter_logo.svg/250px-Jupyter_logo.svg.png" height="50"> Local Python/Jupyter Notebook](examples/SparkMeasure_Jupyter_Python_getting_started.ipynb)

### Examples of sparkMeasure on the CLI
  - Run locally or on hosted resources
    - [![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/LucaCanali/sparkMeasure)

#### Python CLI
  ```
  # Python CLI
  # pip install pyspark
  pip install sparkmeasure
  pyspark --packages ch.cern.sparkmeasure:spark-measure_2.13:0.26

  # Import sparkMeasure
  from sparkmeasure import StageMetrics
  stagemetrics = StageMetrics(spark)
  # Simple one-liner to run a Spark SQL query and measure its performance
  stagemetrics.runandmeasure(globals(), 'spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()')

  # Alternatively, you can use the begin() and end() methods to measure performance
  # Start measuring
  stagemetrics.begin()

  spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()

  # Set a stop point for measuring metrics delta values
  stagemetrics.end()
  # Print the metrics report
  stagemetrics.print_report()
  stagemetrics.print_memory_report()

  # get metrics as a dictionary
  metrics = stagemetrics.aggregate_stage_metrics()
  ```
#### Scala CLI
  ```
  spark-shell --packages ch.cern.sparkmeasure:spark-measure_2.13:0.26

  val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)
  stageMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show())
  ```

The output should look like this:
```
+----------+
|  count(1)|
+----------+
|1000000000|
+----------+

Time taken: 3833 ms

Scheduling mode = FIFO
Spark Context default degree of parallelism = 8

Aggregated Spark stage metrics:
numStages => 3
numTasks => 17
elapsedTime => 1112 (1 s)
stageDuration => 864 (0.9 s)
executorRunTime => 3358 (3 s)
executorCpuTime => 2168 (2 s)
executorDeserializeTime => 892 (0.9 s)
executorDeserializeCpuTime => 251 (0.3 s)
resultSerializationTime => 72 (72 ms)
jvmGCTime => 0 (0 ms)
shuffleFetchWaitTime => 0 (0 ms)
shuffleWriteTime => 36 (36 ms)
resultSize => 16295 (15.9 KB)
diskBytesSpilled => 0 (0 Bytes)
memoryBytesSpilled => 0 (0 Bytes)
peakExecutionMemory => 0
recordsRead => 2000
bytesRead => 0 (0 Bytes)
recordsWritten => 0
bytesWritten => 0 (0 Bytes)
shuffleRecordsRead => 8
shuffleTotalBlocksFetched => 8
shuffleLocalBlocksFetched => 8
shuffleRemoteBlocksFetched => 0
shuffleTotalBytesRead => 472 (472 Bytes)
shuffleLocalBytesRead => 472 (472 Bytes)
shuffleRemoteBytesRead => 0 (0 Bytes)
shuffleRemoteBytesReadToDisk => 0 (0 Bytes)
shuffleBytesWritten => 472 (472 Bytes)
shuffleRecordsWritten => 8

Average number of active tasks => 3.0

Stages and their duration:
Stage 0 duration => 355 (0.4 s)
Stage 1 duration => 411 (0.4 s)
Stage 3 duration => 98 (98 ms)
```

### Memory report
Stage metrics collection mode has an optional memory report command:
```
(scala)> stageMetrics.printMemoryReport
(python)> stagemetrics.print_memory_report()

Additional stage-level executor metrics (memory usage info updated at each heartbeat):

Stage 0 JVMHeapMemory maxVal bytes => 322888344 (307.9 MB)
Stage 0 OnHeapExecutionMemory maxVal bytes => 0 (0 Bytes)
Stage 1 JVMHeapMemory maxVal bytes => 322888344 (307.9 MB)
Stage 1 OnHeapExecutionMemory maxVal bytes => 0 (0 Bytes)
Stage 3 JVMHeapMemory maxVal bytes => 322888344 (307.9 MB)
Stage 3 OnHeapExecutionMemory maxVal bytes => 0 (0 Bytes)
```
Notes:
- this report makes use of per-stage memory (executor metrics) data which is sent by the
  executors at each heartbeat to the driver, there could be a small delay or the order of
  a few seconds between the end of the job and the time the last metrics value is received.

### CLI example for Task Metrics:
This is similar but slightly different from the example above as it collects metrics at the Task-level rather than Stage-level
  ```
  # Scala CLI
  spark-shell --packages ch.cern.sparkmeasure:spark-measure_2.12:0.26

  val taskMetrics = ch.cern.sparkmeasure.TaskMetrics(spark)
  taskMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show())
  ```
  ```
  # Python CLI
  # pip install pyspark
  pip install sparkmeasure
  pyspark --packages ch.cern.sparkmeasure:spark-measure_2.13:0.26

  from sparkmeasure import TaskMetrics
  taskmetrics = TaskMetrics(spark)
  taskmetrics.runandmeasure(globals(), 'spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()')
  ```
---

## Setting Up SparkMeasure with Spark

### Version Compatibility for SparkMeasure

| Spark Version  | Recommended SparkMeasure Version | Scala Version       |
| -------------- |----------------------------------|---------------------|
| Spark 4.x      | 0.26 (latest)                    | Scala 2.13          |
| Spark 3.x      | 0.26 (latest)                    | Scala 2.12 and 2.13 |
| Spark 2.4, 2.3 | 0.19                             | Scala 2.11          |
| Spark 2.2, 2.1 | 0.16                             | Scala 2.11          |

### 📥 Downloading SparkMeasure

To get SparkMeasure, choose one of the following options:

1. **Stable Releases:**

    * Available on [Maven Central Repository](https://mvnrepository.com/artifact/ch.cern.sparkmeasure/spark-measure).

2. **Specific Versions:**

    * Download JAR files from the [sparkMeasure release notes](https://github.com/LucaCanali/sparkMeasure/releases/tag/v0.26).

3. **Latest Development Builds:**

    * Access the latest development builds from [GitHub Actions](https://github.com/LucaCanali/sparkMeasure/actions).

4. **Build from Source:**

    * Clone the repository and use sbt to build: `sbt +package`.

### Setup Examples

#### Spark 4 with Scala 2.13

* **Scala:** `spark-shell --packages ch.cern.sparkmeasure:spark-measure_2.13:0.26`
* **Python:**

  ```bash
  pyspark --packages ch.cern.sparkmeasure:spark-measure_2.13:0.26
  pip install sparkmeasure
  ```

#### Spark 3 with Scala 2.12

* **Scala:** `spark-shell --packages ch.cern.sparkmeasure:spark-measure_2.12:0.26`
* **Python:**

  ```bash
  pyspark --packages ch.cern.sparkmeasure:spark-measure_2.12:0.26
  pip install sparkmeasure
  ```
### Including sparkMeasure in your Spark environment

Choose your preferred method:

* Use the `--packages` option:

  ```bash
  --packages ch.cern.sparkmeasure:spark-measure_2.13:0.26
  ```
* Directly reference the JAR file:

  ```bash
  --jars /path/to/spark-measure_2.13-0.26.jar
  --jars https://github.com/LucaCanali/sparkMeasure/releases/download/v0.26/spark-measure_2.13-0.26.jar
  --conf spark.driver.extraClassPath=/path/to/spark-measure_2.13-0.26.jar
  ```


--
## Running unit tests

To ensure the integrity of the sparkMeasure codebase and validate your setup, you can run the built-in unit tests. These tests are designed to verify core functionality.
[Running sparkMeasure unit tests](docs/Unit-tests.md)

---
## Notes on Spark Metrics
Spark is instrumented with several metrics, collected at task execution, they are described in the documentation:
- [Spark Task Metrics docs](https://spark.apache.org/docs/latest/monitoring.html#executor-task-metrics)

Some of the key metrics when looking at a sparkMeasure report are:
- **elapsedTime:** the time taken by the stage or task to complete (in millisec)
- **executorRunTime:** the time the executors spent running the task, (in millisec). Note this time is cumulative across all tasks executed by the executor.
- **executorCpuTime:** the time the executors spent running the task, (in millisec). Note this time is cumulative across all tasks executed by the executor.
- **jvmGCTime:** the time the executors spent in garbage collection, (in millisec).
- shuffle metrics: several metrics with details on the I/O and time spend on shuffle
- I/O metrics: details on the I/O (reads and writes). Note, currently there are no time-based metrics for I/O operations.

To learn more about the metrics, I advise you set up your lab environment and run some tests to see the metrics in action.
A good place to start with is [TPCDS PySpark](https://github.com/LucaCanali/Miscellaneous/tree/master/Performance_Testing/TPCDS_PySpark) - A tool you can use run TPCDS with PySpark, instrumented with sparkMeasure

---
## Documentation, API, and examples
SparkMeasure is one tool for many different use cases, languages, and environments:
  * [![API Documentation](https://img.shields.io/badge/API-Documentation-brightgreen)](docs/Reference_SparkMeasure_API_and_Configs.md)
    - [SparkMeasure's API and configurations](docs/Reference_SparkMeasure_API_and_Configs.md)


  * **Interactive mode**
    Use sparkMeasure to collect and analyze Spark workload metrics in interactive mode when
    working with shell or notebook environments, such as `spark-shell` (Scala), `PySpark` (Python) and/or
    from `jupyter notebooks`.
    - **[SparkMeasure on PySpark and Jupyter notebooks](docs/Python_shell_and_Jupyter.md)**
    - **[SparkMeasure on Scala shell and notebooks](docs/Scala_shell_and_notebooks.md)**


  * **Batch and code instrumentation**
    Instrument your code with the sparkMeasure API, for collecting, saving,
    and analyzing Spark workload metrics data. Examples and how-to guides:
     - **[Instrument Spark-Python code](docs/Instrument_Python_code.md)**
     - **[Instrument Spark-Scala code](docs/Instrument_Scala_code.md)**
     - **[JMX Exporter integration](docs/Prometheus_through_JMX.md)**
     - See also [Spark_CPU_memory_load_testkit](https://github.com/LucaCanali/Miscellaneous/tree/master/Performance_Testing/Spark_CPU_memory_load_testkit)
       as an example of how to use sparkMeasure to instrument Spark code for performance testing.


  * **Flight Recorder mode**
    SparkMeasure in flight recorder will collect metrics transparently, without any need for you
    to change your code.
    * Metrics can be saved to a file, locally, or to a Hadoop-compliant filesystem
    * or you can write metrics in near-realtime to the following sinks: InfluxDB, Apache Kafka, Prometheus PushPushgateway
    * More details:
      - **[Flight Recorder mode with file sink](docs/Flight_recorder_mode_FileSink.md)**
      - **[Flight Recorder mode with InfluxDB sink](docs/Flight_recorder_mode_InfluxDBSink.md)**
      - **[Flight Recorder mode with Apache Kafka sink](docs/Flight_recorder_mode_KafkaSink.md)**
      - **[Flight Recorder mode with Prometheus Pushgateway sink](docs/Flight_recorder_mode_PrometheusPushgatewaySink.md)**


  * **Limitations and known issues**
    * **Support for Spark Connect**
    SparkMeasure cannot yet provide full integration with [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html)
    because it needs direct access to the `SparkContext` and its listener interface. You can run sparkMeasure in **Flight Recorder** mode
    on the Spark Connect drive* to capture metrics for the entire application, but per-client (Spark Connect client-side)
    metrics are not reported.
    * **Single-threaded applications**
    The sparkMeasure APIs for generating reports using `StageMetric` and `TaskMetric` are best suited
    for a single-threaded driver environment. These APIs capture metrics by calculating deltas between
    snapshots taken at the start and end of an execution. If multiple Spark actions run concurrently on
    the Spark driver, it may result in double-counting of metric values.


  * **Additional documentation and examples**
    - Presentations at Spark/Data+AI Summit:
      - [Performance Troubleshooting Using Apache Spark Metrics](https://databricks.com/session_eu19/performance-troubleshooting-using-apache-spark-metrics)
      - [Apache Spark Performance Troubleshooting at Scale, Challenges, Tools, and Methodologies](http://canali.web.cern.ch/docs/Spark_Summit_2017EU_Performance_Luca_Canali_CERN.pdf)
    - Blog articles:
      - [2018: SparkMeasure, a tool for performance troubleshooting of Apache Spark workloads](https://db-blog.web.cern.ch/blog/luca-canali/2018-08-sparkmeasure-tool-performance-troubleshooting-apache-spark-workloads),
      - [2017: SparkMeasure blog post](http://db-blog.web.cern.ch/blog/luca-canali/2017-03-measuring-apache-spark-workload-metrics-performance-troubleshooting)
  - [TODO list and known issues](docs/TODO_and_issues.md)
  - [TPCDS-PySpark](https://github.com/LucaCanali/Miscellaneous/tree/master/Performance_Testing/TPCDS_PySpark)
    a tool for running the TPCDS benchmark workload with PySpark and instrumented with sparkMeasure

---
## Architecture diagram
![sparkMeasure architecture diagram](docs/sparkMeasure_architecture_diagram.png)

---
## Main concepts underlying sparkMeasure implementation
* The tool is based on the Spark Listener interface. Listeners transport Spark executor
  [Task Metrics](https://github.com/LucaCanali/Miscellaneous/blob/master/Spark_Notes/Spark_TaskMetrics.md)
  data from the executor to the driver.
  They are a standard part of Spark instrumentation, used by the Spark Web UI and History Server for example.
* The tool is built on multiple modules implemented as classes
    * metrics collection and processing can be at the Stage-level or Task-level. The user chooses which mode to use with the API.
    * metrics are can be buffered into memory for real-time reporting, or they can be dumped to an external
    system in the "flight recorder mode".
    * supported external systems are File Systems supported by the Hadoop API, InfluxDB, Apache Kafka, Prometheus Pushgateway.
* Metrics are flattened and collected into local memory structures in the driver (ListBuffer of a custom case class).
  * sparkMeasure in flight recorder mode when using one between the InfluxDB sink, Apache Kafka sink, and Prometheus Pushgateway sink, does not buffer,
  but rather writes the collected metrics directly
* Metrics processing:
  * metrics can be aggregated into a report showing the cumulative values for each metric
  * aggregated metrics can also be returned as a Scala Map or Python dictionary
  * metrics can be converted into a Spark DataFrame for custom querying
* Metrics data and reports can be saved for offline analysis.

### FAQ:
  - Why measuring performance with workload metrics instrumentation rather than just using execution time measurements?
    - When measuring just the jobs' elapsed time, you treat your workload as "a black box" and most often this does
      not allow you to understand the root causes of performance regression.
      With workload metrics you can (attempt to) go further in understanding and perform root cause analysis,
      bottleneck identification, and resource usage measurement.

  - What are Apache Spark task metrics and what can I use them for?
     - Apache Spark measures several details of each task execution, including run time, CPU time,
     information on garbage collection time, shuffle metrics, and task I/O.
     See also Spark documentation for a description of the
     [Spark Task Metrics](https://spark.apache.org/docs/latest/monitoring.html#executor-task-metrics)

  - How is sparkMeasure different from Web UI/Spark History Server and EventLog?
     - sparkMeasure uses the same ListenerBus infrastructure used to collect data for the Web UI and Spark EventLog.
       - Spark collects metrics and other execution details and exposes them via the Web UI.
       - Notably, Task execution metrics are also available through the [REST API](https://spark.apache.org/docs/latest/monitoring.html#rest-api)
       - In addition, Spark writes all details of the task execution in the EventLog file
       (see config of `spark.eventlog.enabled` and `spark.eventLog.dir`)
       - The EventLog is used by the Spark History server + other tools and programs that can read and parse
        the EventLog file(s) for workload analysis and performance troubleshooting, see a [proof-of-concept example of reading the EventLog with Spark SQL](https://github.com/LucaCanali/Miscellaneous/blob/master/Spark_Notes/Spark_EventLog.md)
     - There are key differences that motivate this development:
        - sparkMeasure can collect data at the stage completion-level, which is more lightweight than measuring
        all the tasks, in case you only need to compute aggregated performance metrics. When needed,
        sparkMeasure can also collect data at the task granularity level.
        - sparkMeasure has an API that makes it simple to add instrumentation/performance measurements
         in notebooks and in application code for Scala, Java, and Python.
        - sparkMeasure collects data in a flat structure, which makes it natural to use Spark SQL for
        workload data analysis/
        - sparkMeasure can sink metrics data into external systems (Filesystem, InfluxDB, Apache Kafka, Prometheus Pushgateway)

  - What are known limitations and gotchas?
    - sparkMeasure does not collect all the data available in the EventLog
    - See also the [TODO and issues doc](docs/TODO_and_issues.md)
    - The currently available Spark task metrics can give you precious quantitative information on
      resources used by the executors, however there do not allow to fully perform time-based analysis of
      the workload performance, notably they do not expose the time spent doing I/O or network traffic.
    - Metrics are collected on the driver, which could become a bottleneck. This is an issues affecting tools
      based on Spark ListenerBus instrumentation, such as the Spark WebUI.
      In addition, note that sparkMeasure in the current version buffers all data in the driver memory.
      The notable exception is when using the Flight recorder mode with InfluxDB or
      Apache Kafka or Prometheus Pushgateway sink, in this case metrics are directly sent to InfluxDB/Kafka/Prometheus Pushgateway.
    - Task metrics values are collected by sparkMeasure only for successfully executed tasks. Note that
      resources used by failed tasks are not collected in the current version. The notable exception is
      with the Flight recorder mode with InfluxDB or Apache Kafka or Prometheus Pushgateway sink.
    - sparkMeasure collects and processes data in order of stage and/or task completion. This means that
      the metrics data is not available in real-time, but rather with a delay that depends on the workload
      and the size of the data. Moreover, performance data of jobs executing at the same time can be mixed.
      This can be a noticeable issue if you run workloads with many concurrent jobs.
    - Task metrics are collected by Spark executors running on the JVM, resources utilized outside the
      JVM are currently not directly accounted for (notably the resources used when running Python code
      inside the python.daemon in the case of Python UDFs with PySpark).

  - When should I use Stage-level metrics and when should I use Task-level metrics?
     - Use stage metrics whenever possible as they are much more lightweight. Collect metrics at
     the task granularity if you need the extra information, for example if you want to study
     effects of skew, long tails and task stragglers.

  - How can I save/sink the collected metrics?
     - You can print metrics data and reports to standard output or save them to files, using
      a locally mounted filesystem or a Hadoop compliant filesystem (including HDFS).
     Additionally, you can sink metrics to external systems (such as Prometheus Pushgateway).
     The Flight Recorder mode can sink to InfluxDB, Apache Kafka or Prometheus Pushgateway.

  - How can I process metrics data?
     - You can use Spark to read the saved metrics data and perform further post-processing and analysis.
     See the also [Notes on metrics analysis](docs/Notes_on_metrics_analysis.md).

  - How can I contribute to sparkMeasure?
    - SparkMeasure has already profited from users submitting PR contributions. Additional contributions are welcome.
    See the [TODO_and_issues list](docs/TODO_and_issues.md) for a list of known issues and ideas on what
    you can contribute.
