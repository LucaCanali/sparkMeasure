# SparkMeasure in Flight Recorder mode and sink metrics to a DB backend

Use sparkMeasure in flight recording mode to instrument Spark applications without touching their code.
Flight recorder mode attaches a Spark Listener that collects the metrics while the application runs.
This describes how to sink Spark metrics to an InfluxDB instance.
See also [flight recorder mode with file output](Flight_recorder_mode.md) to see how you can sink the metrics a file instead.

**InfluxDBSink** is a class that extends the SparkListener infrastructure.
It collects and writes Spark metrics and application info in near real-time to an InfluxDB backend
provided by the user. Use this mode to monitor Spark execution workload.
For example this was originally developed to write annotations for a Grafana dashboard with Spark monitoring 
metrics.
How to use: attach the InfluxDBSink to a Spark Context using the extra listener infrastructure. Example:
 - `--conf spark.extraListeners=ch.cern.sparkmeasure.InfluxDBSink`

Configuration for InfluxDBSink is handled with Spark configuration parameters:

 ```
 spark.sparkmeasure.influxdbURL, example value: http://mytestInfluxDB:8086
 spark.sparkmeasure.influxdbUsername (can be empty)
 spark.sparkmeasure.influxdbPassword (can be empty)
 spark.sparkmeasure.influxdbName, defaults to "sparkmeasure"
 spark.sparkmeasure.influxdbStagemetrics, boolean, default is false
 ```

Current implementation depends on "influxdb.java". If you deploy sparkMeasure from maven central,
the dependency is being taken care of.
If you run sparkMeasure from a jar instead, you may need to add the dependency manually
in spark-submit as in:
 - `--packages org.influxdb:influxdb-java:2.14`
 

**InfluxDBExtended** This class extends InfluxDBSink and provides additional and verbose data on each Task execution and metrics
How to use: 
 - `--conf spark.extraListeners=ch.cern.sparkmeasure.InfluxDBSinkExtended`

Note:
 * InfluxDBSink: the amount of data generated is relatively small in most applications: O(number_of_stages)
 * InfluxDBSInkExtended can generate a large amount of data O(Number_of_tasks), use with care

## Example usage:

```
bin/spark-shell --master local[*] --packages ch.cern.sparkmeasure:spark-measure_2.11:0.15 \
--conf spark.sparkmeasure.influxdbURL="http://myInfluxDB:8086" 
--conf spark.extraListeners=ch.cern.sparkmeasure.InfluxDBSink

example, run a test query as:
sql("select count(*) from range(1000) cross join range(1000) cross join range(100)").count
```

## Metrics collected by InfluxDBSink

Connect to InfluxDB shell and explore the metrics data collected:
```
> use sparkmeasure

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
executors_started,applicationId=application_1562699646005_9843
jobs_ended,applicationId=application_1562699646005_9843
jobs_started,applicationId=application_1562699646005_9843
queries_ended,applicationId=application_1562699646005_9843
queries_started,applicationId=application_1562699646005_9843
stages_ended,applicationId=application_1562699646005_9843
stages_started,applicationId=application_1562699646005_9843
task_metrics,applicationId=application_1562699646005_9843
tasks_ended,applicationId=application_1562699646005_9843
tasks_started,applicationId=application_1562699646005_9843

> select * from /queries/
name: queries_ended
time                applicationId                  queryId
----                -------------                  -------
1565093900693000000 application_1562699646005_9843 0
1565093903289000000 application_1562699646005_9843 1

name: queries_started
time                applicationId                  queryId
----                -------------                  -------
1565093900688000000 application_1562699646005_9843 0
1565093903288000000 application_1562699646005_9843 1

> select * from /executors/
name: executors_started
time                applicationId                   executorHost      executorId totalCores
----                -------------                   ------------      ---------- ----------
1565093900688000000 application_1562699646005_9843  host12345.cern.ch 1          2
```

Notes:
 - Stage_metrics data will only be populated if you set `--conf spark.sparkmeasure.influxdbStagemetrics=true`
 - Task metrics for `task_metrics`, `tasks_ended` and `tasks_started` will only be populated if you use InfluxDBExtended class instead of InfluxDbSink

TODO: document all metrics and fields logged. Stop gap: see source code for influxdbsink.scala.
  
## Use cases

InfluxDBSInk: The original use case to develop this feature is to extend the Spark performance dashboard
with annotations for queries, jobs and stages.  
Link on [how to build a Spark monitoring dashboard with InfluxDB and Grafana](http://db-blog.web.cern.ch/blog/luca-canali/2019-02-performance-dashboard-apache-spark) 
Annotations in Grafana using InfluxDB: 
 - define an InfluxDB source
 - define the annotation queries, for example for queries: `select queryId from queries_started where $timeFilter and applicationId =~ /$ApplicationId/` 

InfluxDBSinkExtended: This can be used to build a performance dashboard with full task execution details.
It can also be used for performance analysis using data in InfluxDB series and Influx SQL-like language for querying.
However beware of the impact
of collecting all tasks metrics if the number of tasks is significant.
