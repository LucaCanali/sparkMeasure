## Notes on how to use sparkMeasure in Flight Recorder mode

This is for instrumenting Spark applications without touching their code. Just add an extra custom listener that will 
record the metrics of interest and save to a file at the end of the application.
* For recording stage metrics: <code>--conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics</code>
* For recording task-level metrics: <code>--conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderTaskMetrics</code>

To post-process the saved metrics you will need to deserialize objects saved by the flight mode. This is an example of
how to do that using the supplied helper object sparkmeasure.Utils
```scala
val m1 = ch.cern.sparkmeasure.Utils.readSerializedStageMetrics("/tmp/stageMetrics.serialized")
m1.toDF.show
```
