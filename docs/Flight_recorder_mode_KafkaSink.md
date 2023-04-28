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
```

This code depends on "kafka-clients". If you deploy sparkMeasure from maven central,
the dependency is being taken care of.
If you run sparkMeasure from a jar instead, you may need to add the dependency manually
in spark-submit as in:
 - `--packages org.apache.kafka:kafka-clients:3.3.1`

## Use cases

- The original use case to develop KafkaSink was to extend Spark monitoring, but dumping execution metrics into Kafka 
  performance dashboard and use them to populate a Grafana dashboard.


## Example of how to use KafkaSink

- Start Apache Kafka. 
  - This example uses Kafka configured as in the getting started instructions at
    [Apache Kafka quickstart](https://kafka.apache.org/quickstart)
    - download Apache Kafka and extract it (see instructions in the link above)
    - start zookepeer: `bin/zookeeper-server-start.sh config/zookeeper.properties`
    - start kafka broker `bin/kafka-server-start.sh config/server.properties`

- Start Spark with sparkMeasure and attach the KafkaSink Listener
   -Note: make sure there is no firewall blocking connectivity between the driver and
     the Kafka broker ("my_kafka_server" in the example below)
```
# edit my_kafka_server with the Kafka broker server name
bin/spark-shell \
--conf spark.extraListeners=ch.cern.sparkmeasure.KafkaSink \
--conf spark.sparkmeasure.kafkaBroker=my_kafka_server:9092 \
--conf spark.sparkmeasure.kafkaTopic=metrics
--packages ch.cern.sparkmeasure:spark-measure_2.12:0.23
```

- Look at the metrics being written to Kafka:
```
# edit my_kafka_server with the Kafka broker server name
bin/kafka-console-consumer.sh --topic metrics --from-beginning --bootstrap-server my_kafka_server:9092


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
