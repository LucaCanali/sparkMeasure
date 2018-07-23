# Use sparkMeasure with scala shell or notebooks

### See also [README](../README.md) for an introduction to sparkMeasure and its architecture

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

2. This is an alternative way to collect and print metrics (Scala):
```scala
val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark) 
stageMetrics.begin()

...execute one or more Spark jobs...

stageMetrics.end()
stageMetrics.printReport()
stageMetrics.printAccumulables
```

3. Print additional accumulables metrics (including SQL metrics) collected at stage-level, Scala:
```scala
stageMetrics.printAccumulables()
```

4. Collect and report Task metrics, Scala:
```scala
val taskMetrics = ch.cern.sparkmeasure.TaskMetrics(spark)
taskMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show)
```

# As an alternative to using begin() and end(), you can run the following:
df = taskMetrics.createTaskMetricsDF("PerfTaskMetrics")
spark.sql("select * from PerfTaskMetrics").show()
df.show()
taskMetrics.saveData(df, "taskmetrics_test1", "json")