# SparkMeasure Flight Recorder mode - Prometheus Pushgateway Sink

Use sparkMeasure in flight recorder mode to instrument Spark applications without touching their code.
Flight recorder mode attaches a Spark Listener that collects the metrics while the application runs.
This describes how to sink Spark metrics to a Prometheus Gateway.

## PushGatewaySink 

**PushGatewaySink** is a class that extends the Spark listener infrastructure to collect metrics and 
write them to a Prometheus Gateway endpoint. PushGatewaySink collects and writes Spark metrics
and application info in near real-time to a Prometheus Gateway instance provided by the user.
Use this mode to monitor Spark execution workload.
Notes:
 - Currently, PushGatewaySink collects data at the Stage level (StageMetrics). 
   Task-level metrics are not collected.
 - The amount of data generated is relatively small in most applications; it is O(number_of_stages)
  
How to use: attach the PrometheusGatewaySink to a Spark Context using the listener infrastructure. Example:
  - `--conf spark.extraListeners=ch.cern.sparkmeasure.PushGatewaySink`
  
Configuration for PushGatewaySink is handled using Spark configuration parameters.  
Note: you can add configuration using `--config` options when using spark-submit  
or use the `.config` method when allocating the Spark Session in Scala/Python/Java), as usual.  

**Configurations:**    
```
Option 1 (recommended) Start the listener for PushGatewaySink: 
--conf spark.extraListeners=ch.cern.sparkmeasure.PushGatewaySink

Configuration - PushGatewaySink parameters:

--conf spark.sparkmeasure.pushgateway=SERVER:PORT 
       Example: --conf spark.sparkmeasure.pushgateway=localhost:9091
--conf spark.sparkmeasure.pushgateway.jobname=JOBNAME // default value is pushgateway
       Example: --conf spark.sparkmeasure.pushgateway.jobname=myjob1
--conf spark.sparkmeasure.pushgateway.http.connection.timeout=TIME_IN_MS // default value is 5000
       Example: --conf spark.sparkmeasure.pushgateway.http.connection.timeout=150
--conf spark.sparkmeasure.pushgateway.http.read.timeout=TIME_IN_MS // default value is 5000
       Example: --conf spark.sparkmeasure.pushgateway.http.read.timeout=150
```

## Use case

- The use case for PushGatewaySink is to extend Spark monitoring, by writing execution metrics into Prometheus via the Pushgateway,
  as Prometheus has a pull-based architecture. You'll need to configure Prometheus to pull metrics from the Pushgateway.
  You'll also need to set up a performance dashboard from the metrics collected by Prometheus.


## Example of how to use Prometheus PushGatewaySink

- Start the Prometheus Pushgateway 
  - Download and start the Pushgateway, from the [Prometheus download page](https://prometheus.io/download/)
  
- Start Spark with sparkMeasure and attach the PushGatewaySink listener
   
- *Note*: make sure there is no firewall blocking connectivity between the driver and
     the Pushgateway
```
Examples:  
bin/spark-shell \
--conf spark.extraListeners=ch.cern.sparkmeasure.PushGatewaySink \
--conf spark.sparkmeasure.pushgateway=localhost:9091 \
--packages ch.cern.sparkmeasure:spark-measure_2.12:0.25
```

- Look at the metrics being written to the Pushgateway
  - Use the Web UI to look at the metrics being written to the Pushgateway
  - Open a web browser and go to the WebUI, for example: http://localhost:9091/metrics
  - You should see the metrics being written to the Pushgateway as jobs are run in Spark
```
