# SparkMeasure in Flight Recorder mode

Use sparkMeasure in flight recording mode to instrument Spark applications without touching their code.
Flight recorder mode attaches a Spark Listener that collects the metrics while the application runs.
There are 2 different levels of granularity: stage aggregation, with FlightRecorderStageMetrics and
raw task-level metrics with FlightRecorderTaskMetrics.
The resulting data can be saved to a file and/or printed to stdout.


### Recording stage metrics in Flight Recorder mode
To record metrics at the stage execution level granularity add these conf to spark-submit: 
   ```
   --packages ch.cern.sparkmeasure:spark-measure_2.11:0.14
   --conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics
   ```
Additional parameters are:
   ```
   --conf spark.sparkmeasure.outputFormat=<format> //valid values: javar,json,json_to_hadoop, the default is"json"
   --conf spark.sparkmeasure.outputFilename=<output file> //default: "/tmp/stageMetrics_flightRecorder"
   --conf spark.sparkmeasure.printToStdout=<true|false> // default false. Set to true to print JSON serialized metrics
   ```
**note**:
  - `json` and `java` serialization formats, write to the driver local filesystem
  - `json_to_hadoop`, writes to JSON serialized metrics to  HDFS or to an Hadoop compliant filesystem, such as s3a
    
###FlightRecorderStageMetrics examples ###
Python, runs pi.py example and records metrics to `/tmp/stageMetrics_flightRecorder` in json format:
```
bin/spark-submit --master local[*] --packages ch.cern.sparkmeasure:spark-measure_2.11:0.14 \
--conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics examples/src/main/python/pi.py
```

Scala example,  
- same example as above, in addition use a custom output filename
- print metrics also to stdout
```
bin/spark-submit --master local[*] --packages ch.cern.sparkmeasure:spark-measure_2.11:0.14 \
--class org.apache.spark.examples.SparkPi \
--conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics \
--conf spark.sparkmeasure.printToStdout=true \
--conf spark.sparkmeasure.stagemetricsFilename="/tmp/myout1_$(date +%s).json" \
examples/jars/spark-examples_2.11-2.4.3.jar 10
```

Example of how to Write output to HDFS:  
(note: source the Hadoop environment)
```
bin/spark-submit --master yarn --packages ch.cern.sparkmeasure:spark-measure_2.11:0.14 \
--conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics \
--conf spark.sparkmeasure.outputFormat=json_to_hadoop \
--conf spark.sparkmeasure.stagemetricsFilename="hdfs://myclustername/user/luca/test/myout1_$(date +%s).json" \
examples/src/main/python/pi.py
```

Example, use spark-3.0.0 (snapshot from master), Kubernetes, Scala 2.12 and write output to S3:  
(note: export KUBECONFI=... + setup Hadoop environment)
```
bin/spark-submit --master k8s://https://XXX.XXX.XXX.XXX --deploy-mode client --conf spark.executor.instances=3 \
--conf spark.executor.cores=2 --executor-memory 6g --driver-memory 8g \
--conf spark.kubernetes.container.image=gitlab-registry.cern.ch/canali/testregistry1/spark:v3.0.0_20190529_hadoop32 \
--packages org.apache.hadoop:hadoop-aws:3.2.0,ch.cern.sparkmeasure:spark-measure_2.11:0.14 \
--conf spark.hadoop.fs.s3a.secret.key="YYY..." \
--conf spark.hadoop.fs.s3a.access.key="ZZZ..." \
--conf spark.hadoop.fs.s3a.endpoint="https://s3.cern.ch" \
--conf spark.hadoop.fs.s3a.impl="org.apache.hadoop.fs.s3a.S3AFileSystem" \
--conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics \
--conf spark.sparkmeasure.outputFormat=json_to_hadoop \
--conf spark.sparkmeasure.outputFilename="s3a://test/myout1_$(date +%s).json" \
--class org.apache.spark.examples.SparkPi \
examples/jars/spark-examples_2.12-3.0.0-SNAPSHOT.jar 10
```


### Recording metrics at Task granularity in Flight Recorder mode
To record metrics at the task execution level granularity add these conf to spark-submit.
This can potentially generate large amounts of data in the driver. 
Consider using stage-level granularity first.

   ```
   --conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderTaskMetrics
   ```
Additional parameters are:

   ```
   --conf spark.sparkmeasure.outputFormat=<format> //valid values: javar,json,json_to_hadoop, the default is"json"
   --conf spark.sparkmeasure.outputFilename=<output file> //default: "/tmp/taskMetrics_flightRecorder"
   --conf spark.sparkmeasure.printToStdout=<true|false> // default false. Set to true to print JSON serialized metrics
   ```
**note**:
  - `json` and `java` serialization formats, write to the driver local filesystem
  - `json_to_hadoop`, writes to JSON serialized metrics to  HDFS or to an Hadoop compliant filesystem, such as s3a
    
  
### Metrics post-processing

To post-process the saved metrics you will need to deserialize objects saved by the flight mode. 
This is an example of how to do that using the supplied helper object sparkmeasure.Utils

```
bin/spark-shell  --packages ch.cern.sparkmeasure:spark-measure_2.11:0.14

val myMetrics = ch.cern.sparkmeasure.IOUtils.readSerializedStageMetricsJSON("/tmp/stageMetrics_flightRecorder")
// use ch.cern.sparkmeasure.IOUtils.readSerializedStageMetrics("/tmp/stageMetrics.serialized") for java serialization
myMetrics.toDF.show()
```

### Notes

- If you are deploying applications using cluster mode, note that metrics serialized with json or java
 are written by the driver into the local filesystem.  You could use a network filesystem mounted on the driver/cluster for convenience. 
 You can also use json_to_hadoop serialization to write the metrics to HDFS or and Hadoop-compliant filesystem such as S3.

- The flight recorder method is similar to what Spark already does with the event log, where metrics are
stored: see also [Spark documentation](https://spark.apache.org/docs/latest/monitoring.html) for spark.eventLog.enabled and spark.eventLog.dir and for details on 
the Spark History Server.  
See also this note with a few tips on how to read event log files(https://github.com/LucaCanali/Miscellaneous/blob/master/Spark_Notes/Spark_EventLog.md)  

- For metrics analysis see also notes at  [Notes_on_metrics_analysis.md](Notes_on_metrics_analysis.md) for a few examples.
