# Use sparkMeasure with Scala shell or notebooks

Notes on how to use sparkMeasure to collect Spark workload metrics when Scala shell or a Scala notebook.
See also [README](../README.md) for an introduction to sparkMeasure and its architecture.

### Download and build sparkMeasure (optional)

 - Optionally build sparkMeasure jar (the alternative is to use a released varsion available on Maven Central).
    ```scala
    git clone https://github.com/lucacanali/sparkmeasure
    cd sparkmeasure
    sbt package
    ls -l target/scala-2.11/spark-measure*.jar  # location of the compiled jar
 
    # Run as in one of these examples:
    bin/spark-shell --jars path>/spark-measure_2.11-0.12-SNAPSHOT.jar
    
    #alternative:
    bin/spark-shell --conf spark.driver.extraClassPath=<path>/spark-measure_2.11-0.12-SNAPSHOT.jar
    ```
  
### Deploy sparkMeasure from Maven Central 

- The alternative, see paragraph above, is to build a jar from master.
    ```scala
    bin/spark-shell --packages ch.cern.sparkmeasure:spark-measure_2.11:0.11

    // or just use the jar (it is only needed in the driver) as in:
    bin/spark-shell --conf spark.driver.extraClassPath=<path>/spark-measure_2.11-0.12-SNAPSHOT.jar
   ```

### Collect and print stage metrics
 
1. Measure metrics at the Stage level, a basic exaple:
    ```
    bin/spark-shell --packages ch.cern.sparkmeasure:spark-measure_2.11:0.11
    
    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark) 
    stageMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show)
    ```

    Example output:
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

2. An alternative way to collect and print metrics:
    ```scala
    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark) 
    stageMetrics.begin()
    
    ...execute one or more Spark jobs...
    
    stageMetrics.end()
    stageMetrics.printReport()
    ```

3. Print additional accumulables metrics (including SQL metrics) collected at stage-level, Scala:
    ```scala
    stageMetrics.printAccumulables()
    ```

### Task metrics

Collecting Spark task metrics at the granularity of each task completion has additional overhead
compare to collecting at the stage completion level, therefore this option should only be used if you need data with this finer granularity, for example because you want
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
Another option is to export the metrics to an external system, such as [Prometheus Pushgateway](prometheus.md) 
  
- Example on how to export raw Stage Metrics metrics data in json format
    ```scala
    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark) 
    stageMetrics.runAndMeasure( ...your workload here ... )
  
    val df = stageMetrics.createStageMetricsDF("PerfStageMetrics")
    df.show()
    stageMetrics.saveData(df.orderBy("jobId", "stageId"), "/tmp/stagemetrics_test1")

    val accumulablesDF = stagemetrics.createAccumulablesDF("AccumulablesStageMetrics")
    stageMetrics.saveData(accumulablesDF, "/tmp/stagemetrics_accumulables_test1")
    ```

- Example, save aggregated metrics (as found in the printReport output) in json format

    ```scala
    val df = stageMetrics.createStageMetricsDF("PerfStageMetrics")
    val aggregatedDF = stageMetrics.aggregateStageMetrics("PerfStageMetrics")
    aggregatedDF.show()
    stageMetrics.saveData(aggregatedDF, "/tmp/stagemetrics_report_test2")
    ```

- Example on how to export raw Task Metrics data in json format
    ```scala
    val df = taskMetrics.createTaskMetricsDF("PerfTaskMetrics")
    spark.sql("select * from PerfTaskMetrics").show()
    df.show()
    taskMetrics.saveData(df.orderBy("jobId", "stageId", "index"), "<path>/taskmetrics_test3")
    ```
