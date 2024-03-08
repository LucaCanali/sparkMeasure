# SparkMeasure Flight Recorder Mode - InfluxDB Sink

Use sparkMeasure in flight recorder mode to instrument Spark applications without touching their code.
Flight recorder mode attaches a Spark Listener that collects the metrics while the application runs.
This describes how to sink Spark metrics to an InfluxDB instance.  
Note this is for InfluxDB version 1.x, for version 2.x some changes are needed.  
You can use this also with VictoriaMetrics, ingesting the InfluxDB line protocol.  

## InfluxDBSink and InfluxDBSinkExtended 

**InfluxDBSink** is a class that extends the SparkListener infrastructure.  
It collects and writes Spark metrics and application info in near real-time to an InfluxDB backend
provided by the user. Use this mode to monitor Spark execution workload.  
**InfluxDBSinkExtended** Extends the functionality to record metrics for each executed Task  

Notes:
- InfluxDBSink: the amount of data generated is relatively small in most applications: O(number_of_stages)
- InfluxDBSInkExtended can generate a large amount of data O(Number_of_tasks), use with care
  
How to use: attach the InfluxDBSink to a Spark Context using the extra listener infrastructure. Example:
  - `--conf spark.extraListeners=ch.cern.sparkmeasure.InfluxDBSink`
  
Configuration for InfluxDBSink is handled with Spark configuration parameters.  
Note: you can add configuration using --config option when using spark-submit  
use the .config method when allocating the Spark Session in Scala/Python/Java).  
Configurations:  
 ```
Option 1 (recommended) Start the listener for InfluxDBSink: 
--conf spark.extraListeners=ch.cern.sparkmeasure.InfluxDBSink

As an alternative, start the listener for InfluxDBSink+InfluxDBSinkExtended:
--conf spark.extraListeners=ch.cern.sparkmeasure.InfluxDBSink,ch.cern.sparkmeasure.InfluxDBSinkExtended

Configuration - InfluxDBSink parameters:

--conf spark.sparkmeasure.influxdbURL (default "http://localhost:8086")
--conf spark.sparkmeasure.influxdbUsername (default "")
       Note: username and password can be empty only if InfluxDB runs with auth-enabled=false
--conf spark.sparkmeasure.influxdbPassword (default "")
--conf spark.sparkmeasure.influxdbName (default "sparkmeasure")
       Note: the DB will be created if it does not exist already
--conf spark.sparkmeasure.influxdbStagemetrics, (boolean, default is false)
       Note: when this is true stage metrics will be collected too
--conf spark.sparkmeasure.influxdbEnableBatch, boolean, default true
       Note: this is to improve write performance, 
             but it requires to explicitly stopping Spark Session for clean exit: spark.stop()
             consider setting it to false if this is an issue
 ```

Note: The current implementation depends on "influxdb.java". If you deploy sparkMeasure from maven central,
the dependency is being taken care of.  
If you run sparkMeasure from a jar instead, you may need to add the dependency manually
in spark-submit/spark-shell as in:
 - `--packages org.influxdb:influxdb-java:2.14`
Note currently we need to use influxdb-java version 2.14, as newer versions generate jar conflicts (tested up to Spark version 3.3.1)

## Use cases

- InfluxDBSink can be used as a monitoring application for Spark, notably setting `spark.sparkmeasure.influxdbStagemetrics` to true
- The original use case to develop InfluxDBSInk was to extend the Spark performance dashboard
  with annotations for queries, jobs and stages.  
  See also [how to build a Spark monitoring dashboard with InfluxDB and Grafana](http://db-blog.web.cern.ch/blog/luca-canali/2019-02-performance-dashboard-apache-spark)
- InfluxDBSinkExtended: This can be used to build a performance dashboard with full task execution details.
  It can also be used for performance analysis using data in InfluxDB series and Influx SQL-like language for querying.
  However, beware of the impact of collecting all tasks metrics if the number of tasks is significant.


## Example of how to use InfluxDBSink

- Start InfluxDB. 
  - This example uses docker containers and will listen on port 8086 of your local machine
  - This example uses InfluxDB version 1.8 (using InfluxDB version 2 requires some changes in the example)
```
# Alternative 1. 
# Use this if you plan to use the Spark dashboard as in
# https://github.com/cerndb/spark-dashboard 
docker run --name influx --network=host -d lucacanali/spark-dashboard:v01

# Alternative 2.
# Start InfluxDB, for example using a docker image 
docker run --name influx --network=host -d influxdb:1.8.10
```

- Start Spark with the InfluxDBSink Listener
```
bin/spark-shell \
  --conf spark.sparkmeasure.influxdbURL="http://localhost:8086" \
  --conf spark.extraListeners=ch.cern.sparkmeasure.InfluxDBSink,ch.cern.sparkmeasure.InfluxDBSinkExtended \
  --conf spark.sparkmeasure.influxdbStagemetrics=true
  --packages ch.cern.sparkmeasure:spark-measure_2.12:0.24

// run a Spark job, this will produce metrics  
spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show
```

## Explore the metrics collected

- Notes:
  - Stage_metrics data will only be populated if you set `--conf spark.sparkmeasure.influxdbStagemetrics=true`
  - Task metrics for `task_metrics`, `tasks_ended` and `tasks_started` will only be populated if you use InfluxDBExtended class instead of InfluxDbSink
- Connect to InfluxDB shell and explore the metrics collected:
```
# Use this if you started InfluxDB uising a docker image
docker exec -it influx /bin/bash

# Start the InfluxDB CLI
/usr/bin/influx

> use sparkmeasure
Using database sparkmeasure
> show measurements
name: measurements
name
----
executors_started
jobs_ended
jobs_started
queries_ended
queries_started
stages_ended
stages_started
task_metrics
tasks_ended
tasks_started

> show series
key
---
executors_started,applicationId=noAppId
jobs_ended,applicationId=local-1660122150941
jobs_started,applicationId=local-1660122150941
queries_ended,applicationId=local-1660122150941
queries_started,applicationId=local-1660122150941
stage_metrics,applicationId=local-1660122150941
stages_ended,applicationId=local-1660122150941
stages_started,applicationId=local-1660122150941
task_metrics,applicationId=local-1660122150941
tasks_ended,applicationId=local-1660122150941
tasks_started,applicationId=local-1660122150941
> select * from queries_started
name: queries_started
time                applicationId       description          queryId
----                -------------       -----------          -------
1660122211786000000 local-1660122150941 show at <console>:23 0
> select * from /executors/
name: executors_started
time                applicationId executorHost              executorId totalCores
----                ------------- ------------              ---------- ----------
1660122151091000000 noAppId       pcitdbgpu1.dyndns.cern.ch driver     8
> select * from stage_metrics
name: stage_metrics
time                applicationId       attemptNumber bytesRead bytesWritten completionTime executorCpuTime executorDeserializeCpuTime executorDeserializeTime executorRunTime failureReason jvmGCTime memoryBytesSpilled peakExecutionMemory recordsRead recordsWritten resultSerializationTime resultSize shuffleBytesWritten shuffleFetchWaitTime shuffleLocalBlocksFetched shuffleLocalBytesRead shuffleRecordsRead shuffleRecordsWritten shuffleRemoteBlocksFetched shuffleRemoteBytesRead shuffleRemoteBytesReadToDisk shuffleTotalBlocksFetched shuffleTotalBytesRead shuffleWriteTime stageId submissionTime
----                -------------       ------------- --------- ------------ -------------- --------------- -------------------------- ----------------------- --------------- ------------- --------- ------------------ ------------------- ----------- -------------- ----------------------- ---------- ------------------- -------------------- ------------------------- --------------------- ------------------ --------------------- -------------------------- ---------------------- ---------------------------- ------------------------- --------------------- ---------------- ------- --------------
1660122213061000000 local-1660122150941 0             0         0            1660122213061  82265168        1039200151                 3171                    301                           144       0                  0                   1000        0              7                       12119      0                   0                    0                         0                     0                  0                     0                          0                      0                            0                         0                     0                0       1660122212450
1660122213630000000 local-1660122150941 0             0         0            1660122213630  2180200879      49937229                   155                     2491                          0         0                  0                   1000        0              7                       16134      472                 0                    0                         0                     0                  8                     0                          0                      0                            0                         0                     65588303         1       1660122213240
1660122213764000000 local-1660122150941 0             0         0            1660122213764  38334075        2650585                    2                       39                            0         0                  0                   0           0              0                       2667       0                   0                    8                         472                   8                  0                     0                          0                      0                            8                         472                   0                3       1660122213711
```

## List of the Task Metrics collected by InfluxDBSink

- stageId
- attemptNumber
- failureReason
- submissionTime
- completionTime
- executorRunTime
- executorCpuTime
- executorDeserializeCpuTime
- executorDeserializeTime
- jvmGCTime
- memoryBytesSpilled
- peakExecutionMemory
- resultSerializationTime
- resultSize
- inputMetrics.bytesRead
- inputMetrics.recordsRead
- outputMetrics.bytesWritten
- outputMetrics.recordsWritten
- shuffleReadMetrics.totalBytesRead
- shuffleReadMetrics.remoteBytesRead
- shuffleReadMetrics.remoteBytesReadToDisk
- shuffleReadMetrics.localBytesRead
- shuffleReadMetrics.totalBlocksFetched
- shuffleReadMetrics.localBlocksFetched
- shuffleReadMetrics.remoteBlocksFetched
- shuffleReadMetrics.recordsRead
- shuffleReadMetrics.fetchWaitTime
- shuffleWriteMetrics.bytesWritten
- shuffleWriteMetrics.recordsWritten
- shuffleWriteMetrics.writeTime
