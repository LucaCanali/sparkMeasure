# Notes on how to use sparkMeasure in Flight Recorder mode

This is for instrumenting Spark applications without touching their code. 
SparkMeasure can be used to add an extra custom listener that will 
record the Spark metrics and save them to a file at the end of the application for later processing.


### Recording stage metrics in Flight Recorder mode
To record metrics at the stage execution level granularity add these conf to spark-submit: 
   ```
   --packages ch.cern.sparkmeasure:spark-measure_2.11:0.13
   --conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics
   ```
Additional parameters are:
   ```
   --conf spark.executorEnv.stageMetricsFileName"<outputfile_path_and_name>" (default is "/tmp/stageMetrics.serialized")
   --conf spark.executorEnv.taskMetricsFormat="file format" ("java" or "json", default is "java")
   ```

### Recording metrics at Task granularity in Flight Recorder mode
To record metrics at the task execution level granularity add these conf to spark-submit.
This can potentially generate large amounts of data in the driver. 
Consider using stage-level granularity first.

   ```
   --conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderTaskMetrics</code>
   ```
Additional parameters are:
   ```
   --conf spark.executorEnv.stageMetricsFileName"<outputfile_path_and_name>" (default is "/tmp/stageMetrics.serialized")
   --conf spark.executorEnv.taskMetricsFormat="file format" ("java" or "json", default is "java")
   ```

### Metrics post-processing

To post-process the saved metrics you will need to deserialize objects saved by the flight mode. 
This is an example of how to do that using the supplied helper object sparkmeasure.Utils

```scala
bin/spark-shell  --packages ch.cern.sparkmeasure:spark-measure_2.11:0.13

val myMetrics = ch.cern.sparkmeasure.Utils.readSerializedStageMetrics("/tmp/stageMetrics.serialized")
myMetrics.toDF.show()
```

### Notes

- The flight recorder method is similar to what Spark already does with the event log, where metrics are
stored: see also [Spark documentation](https://spark.apache.org/docs/latest/monitoring.html) for spark.eventLog.enabled and spark.eventLog.dir and for details on 
the Spark History Server.  
See also this note with a few tips on how to read event log files(https://github.com/LucaCanali/Miscellaneous/blob/master/Spark_Notes/Spark_EventLog.md)  

- For metrics analysis see also notes at  [Notes_on_metrics_analysis.md](Notes_on_metrics_analysis.md) for a few examples.

- If you are deploying applications using cluster mode, note that the serialized metrics
 are written by the driver and therefore the path is local to the driver process.
 You could use a network filesystem mounted on the driver/cluster for convenience. 