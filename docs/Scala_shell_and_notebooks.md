# Use sparkMeasure with Scala shell or notebooks

Notes on how to use sparkMeasure to collect Spark workload metrics when Scala shell or a Scala notebook.
See also [README](../README.md) for an introduction to sparkMeasure and its architecture.

 
### Run sparkMeasure using the packaged version from Maven Central 

- The alternative, see paragraph above, is to build a jar from master.
    ```
    bin/spark-shell --packages ch.cern.sparkmeasure:spark-measure_2.12:0.19

    // or just download and use the jar (it is only needed in the driver) as in:
    bin/spark-shell --conf spark.driver.extraClassPath=<path>/spark-measure_2.12-0.19.jar
   ```

### Download and build sparkMeasure (optional)

 - If you prefer to build from the latest development version:
    ```
    git clone https://github.com/lucacanali/sparkmeasure
    cd sparkmeasure
    sbt +package
    ls -l target/scala-2.12/spark-measure*.jar  # location of the compiled jar
 
    # Run as in one of these examples:
    bin/spark-shell --jars <path>/spark-measure_2.12-0.20-SNAPSHOT.jar
    
    # alternative, set classpath for the driver (the jar is only needed in the driver)
    bin/spark-shell --conf spark.driver.extraClassPath=<path>/spark-measure_2.11-0.20-SNAPSHOT.jar
    ```

### Example: collect and print stage metrics with sparkMeasure
 
1. Measure metrics at the Stage level, a basic example:
    ```
    bin/spark-shell --packages ch.cern.sparkmeasure:spark-measure_2.12:0.19
    
    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark) 
    stageMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show)
    ```

    Example output:
```
Scheduling mode = FIFO
Spark Context default degree of parallelism = 8

Aggregated Spark stage metrics:
numStages => 3
numTasks => 17
elapsedTime => 1218 (1 s)
stageDuration => 994 (1.0 s)
executorRunTime => 2625 (3 s)
executorCpuTime => 2224 (2 s)
executorDeserializeTime => 2945 (3 s)
executorDeserializeCpuTime => 1153 (1 s)
resultSerializationTime => 8 (8 ms)
jvmGCTime => 80 (80 ms)
shuffleFetchWaitTime => 0 (0 ms)
shuffleWriteTime => 30 (30 ms)
resultSize => 16220 (15.0 KB)
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

Stages and their duration:
Stage 0 duration => 573 (0.6 s)
Stage 1 duration => 354 (0.4 s)
Stage 3 duration => 67 (67 ms)
```
 
- New in sparkMeasure v01: memory metrics report:
```
> stageMetrics.printMemoryReport

Additional stage-level executor metrics (memory usasge info):

Stage 0 JVMHeapMemory maxVal bytes => 322888344 (307.9 MB)
Stage 0 OnHeapExecutionMemory maxVal bytes => 0 (0 Bytes)
Stage 1 JVMHeapMemory maxVal bytes => 322888344 (307.9 MB)
Stage 1 OnHeapExecutionMemory maxVal bytes => 0 (0 Bytes)
Stage 3 JVMHeapMemory maxVal bytes => 322888344 (307.9 MB)
Stage 3 OnHeapExecutionMemory maxVal bytes => 0 (0 Bytes)
```

2. An alternative way to collect and print metrics:
    ```scala
    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark) 
    stageMetrics.begin()
    
    ...execute one or more Spark jobs...
    
    stageMetrics.end()
    stageMetrics.printReport()
    ```

### Collecting metrics at finer granularity: use Task metrics

Collecting Spark task metrics at the granularity of each task completion has additional overhead
compare to collecting at the stage completion level, therefore this option should only be used if you need data with 
this finer granularity, for example because you want
to study skew effects, otherwise consider using stagemetrics aggregation as preferred choice.

- The API for collecting data at task level is similar to the stage metrics case.
  An example:
    ```scala
    val taskMetrics = ch.cern.sparkmeasure.TaskMetrics(spark)
    taskMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show)
    ```


### Exporting metrics data for archiving and/or further analysis

One simple use case is to make use of the data collected and reported by stagemetrics and taskmetrics 
printReport methods for immediate troubleshooting and workload analysis.  
You also have options to save metrics aggregated as in the printReport output.  
Another option is to export the metrics to an external system, such as [Prometheus Pushgateway](Prometheus.md) 
  
- Example on how to export raw Stage Metrics metrics into a DataFrame and sabe data in json format
    ```scala
    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark) 
    stageMetrics.runAndMeasure( ...your workload here ... )
  
    val df = stageMetrics.createStageMetricsDF("PerfStageMetrics")
    df.show()
    stageMetrics.saveData(df.orderBy("jobId", "stageId"), "/tmp/stagemetrics_test1")
    ```

- Example, save aggregated metrics (as found in the printReport output) in json format

    ```scala
    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark) 
    stageMetrics.runAndMeasure( ...your workload here ... )
    
    val df = stageMetrics.createStageMetricsDF("PerfStageMetrics")
    val aggregatedDF = stageMetrics.aggregateStageMetrics("PerfStageMetrics")
    aggregatedDF.show()
    stageMetrics.saveData(aggregatedDF, "/tmp/stagemetrics_report_test2")
    ```
 Aggregated data in name,value format:
 ```scala
    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark) 
    stageMetrics.runAndMeasure( ...your workload here ... )
    
    val df = stageMetrics.aggregateStageMetrics.toList.toDF("name","value")
    stageMetrics.saveData(df, "/tmp/stagemetrics_report_test3")
 ```

- Example on how to export raw Task Metrics data in json format
    ```scala
    val df = taskMetrics.createTaskMetricsDF("PerfTaskMetrics")
    spark.sql("select * from PerfTaskMetrics").show()
    df.show()
    taskMetrics.saveData(df.orderBy("jobId", "stageId", "index"), "<path>/taskmetrics_test3")
    ```
