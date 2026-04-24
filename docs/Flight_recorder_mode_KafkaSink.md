# SparkMeasure Flight Recorder mode - Apache Kafka Sink

Use sparkMeasure in flight recorder mode to instrument Spark applications without touching their code.
Flight recorder mode attaches a Spark Listener that collects the metrics while the application runs.
This describes how to sink Spark metrics to Apache Kafka.

## KafkaSink and KafkaSinkExtended 

**KafkaSink** is a class that extends the SparkListener infrastructure.  
It collects and writes Spark metrics and application info in near real-time to an Apache Kafka backend
provided by the user. Use this mode to monitor Spark execution workload.  
**KafkaSinkExtended** Extends the functionality to record metrics for each executed Task  

Notes:
- KafkaSink: the amount of data generated is relatively small in most applications: O(number_of_stages)
- KafkaSinkExtended can generate a large amount of data O(Number_of_tasks), use with care

## KafkaSinkV2 and KafkaSinkV2Extended

**KafkaSinkV2** is an enhanced version of KafkaSink that adds application-level aggregated metrics and custom labels support.  
It collects all stage/executor/query metrics from the base KafkaSink plus additional application lifecycle events and counters.  
**KafkaSinkV2Extended** extends KafkaSinkV2 to also record detailed metrics for each executed Task.

**Compatibility Note:** KafkaSinkV2 is backward compatible with KafkaSink in terms of configuration and basic event types.
Existing consumers of KafkaSink events will continue to work as expected. KafkaSinkV2 adds two new event types
(`applications_started` and `applications_ended`) with additional metadata that existing consumers can safely ignore
if not needed.

### Key Differences from KafkaSink

1. **Application-Level Metrics:** KafkaSinkV2 emits `applications_started` and `applications_ended` events with aggregated counters
2. **Custom Labels:** Support for custom metadata labels via `spark.sparkmeasure.appLabels.*` configuration
3. **Enhanced Application End Event:** Includes executor counts, job/stage/task counters, and selected Spark configurations
4. **Counter Tracking:** Tracks success/failure counts for jobs, stages, and tasks throughout application lifecycle

## Configuration

### KafkaSink / KafkaSinkExtended Configuration
  
How to use: attach the KafkaSink to a Spark Context using the extra listener infrastructure. Example:
  - `--conf spark.extraListeners=ch.cern.sparkmeasure.KafkaSink`
  
Configuration for KafkaSink is handled with Spark configuration parameters.  
Note: you can add configuration using --config option when using spark-submit  
use the .config method when allocating the Spark Session in Scala/Python/Java).  
Configurations:  
 ```
Option 1 (recommended) Start the listener for KafkaSink: 
--conf spark.extraListeners=ch.cern.sparkmeasure.KafkaSink

As an alternative, start the listener for KafkaSink+KafkaSinkExtended:
--conf spark.extraListeners=ch.cern.sparkmeasure.KafkaSink,ch.cern.sparkmeasure.KafkaSinkExtended

Configuration - KafkaSink parameters:

--conf spark.sparkmeasure.kafkaBroker = Kafka broker endpoint URL
       Example: --conf spark.sparkmeasure.kafkaBroker=kafka.your-site.com:9092
--conf spark.sparkmeasure.kafkaTopic = Kafka topic
       Example: --conf spark.sparkmeasure.kafkaTopic=sparkmeasure-stageinfo
       Note: the topic will be created if it does not yet exist
--conf spark.sparkmeasure.kafka.* = Other kafka properties
       Example: --conf spark.sparkmeasure.kafka.ssl.keystore.location=/var/private/ssl/kafka.server.keystore.jks
```

### KafkaSinkV2 / KafkaSinkV2Extended Configuration

How to use: attach the KafkaSinkV2 listener to the Spark Context:

```
# Start the listener for KafkaSinkV2 (recommended):
--conf spark.extraListeners=ch.cern.sparkmeasure.KafkaSinkV2

# Or use KafkaSinkV2Extended for task-level metrics (generates more data):
--conf spark.extraListeners=ch.cern.sparkmeasure.KafkaSinkV2Extended

# Required Kafka configuration (same as KafkaSink):
--conf spark.sparkmeasure.kafkaBroker = Kafka broker endpoint URL
       Example: --conf spark.sparkmeasure.kafkaBroker=kafka.your-site.com:9092
--conf spark.sparkmeasure.kafkaTopic = Kafka topic
       Example: --conf spark.sparkmeasure.kafkaTopic=sparkmeasure-metrics

# Optional - Custom application labels:
--conf spark.sparkmeasure.appLabels.<labelKey> = Custom metadata value
       Example: --conf spark.sparkmeasure.appLabels.project=my-project
       Example: --conf spark.sparkmeasure.appLabels.environment=production
```

This code depends on "kafka-clients". If you deploy sparkMeasure from maven central,
the dependency is being taken care of.
If you run sparkMeasure from a jar instead, you may need to add the dependency manually
in spark-submit as in:
 - `--packages org.apache.kafka:kafka-clients:4.1.0`

## Use cases

- The original use case to develop KafkaSink was to extend Spark monitoring, but dumping execution metrics into Kafka 
  performance dashboard and use them to populate a Grafana dashboard.


## Example of how to use KafkaSink

- Start Apache Kafka. 
  - This example uses Kafka configured as in the getting started instructions at
    [Apache Kafka quickstart](https://kafka.apache.org/quickstart)
    - for example run from Docker image: `docker run -p 9092:9092 apache/kafka:latest`

- Start Spark with sparkMeasure and attach the KafkaSink Listener
   
- *Note*: make sure there is no firewall blocking connectivity between the driver and
     the Kafka broker ("my_kafka_server" in the example below)
```
# edit my_kafka_server with the Kafka broker server name
bin/spark-shell \
--conf spark.extraListeners=ch.cern.sparkmeasure.KafkaSink \
--conf spark.sparkmeasure.kafkaBroker=localhost:9092 \
--conf spark.sparkmeasure.kafkaTopic=metrics
--packages ch.cern.sparkmeasure:spark-measure_2.13:0.27
```

- Look at the metrics being written into Kafka:
```
# edit my_kafka_server with the Kafka broker server name
/opt/kafka/bin/kafka-console-consumer.sh --topic metrics --from-beginning --bootstrap-server localhost:9092


{
  "epochMillis" : 1660057441932,
  "name" : "executors_started",
  "startTime" : 1660057441742,
  "host" : "myhost123.cern.ch",
  "executorId" : "driver",
  "totalCores" : 8,
  "appId" : "noAppId"
}
{
  "queryId" : "0",
  "epochMillis" : 1660057452417,
  "name" : "queries_started",
  "startTime" : 1660057452417,
  "description" : "show at <console>:23",
  "appId" : "local-1660057441489"
}
{
  "epochMillis" : 1660057452974,
  "name" : "jobs_started",
  "startTime" : 1660057452972,
  "jobId" : "0",
  "appId" : "local-1660057441489"
...
```

**Note:** KafkaSinkV2 also emits all standard events like `stages_started`, `stages_ended`, `stage_metrics`, etc., 
just like the original KafkaSink, ensuring backward compatibility.

## Migration Guide: KafkaSink to KafkaSinkV2

If you are currently using KafkaSink and want to migrate to KafkaSinkV2:

1. **Configuration Change:** Simply replace the listener class name:
   ```
   # Old:
   --conf spark.extraListeners=ch.cern.sparkmeasure.KafkaSink
   
   # New:
   --conf spark.extraListeners=ch.cern.sparkmeasure.KafkaSinkV2
   ```

2. **Backward Compatibility:** All existing event types and their schemas remain unchanged. Your existing Kafka consumers will continue to work.

3. **New Events:** KafkaSinkV2 adds two new event types (`applications_started` and `applications_ended`). If your consumers don't need these, they can simply ignore events with these names.

4. **Optional Custom Labels:** Add custom labels for better filtering and organization:
   ```
   --conf spark.sparkmeasure.appLabels.project=my-project
   --conf spark.sparkmeasure.appLabels.environment=staging
   ```

5. **Benefits:** You gain application-level aggregated metrics, custom labels, and selected Spark configurations without losing any existing functionality.
