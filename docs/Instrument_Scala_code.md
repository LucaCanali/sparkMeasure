# How to use sparkMeasure to instrument Scala code

SparkMeasure can be used to instrument parts of your Scala code to measure Apache Spark workload.
Use this for example for performance troubleshooting, application instrumentation, workload studies, etc.

### Example code 
 
You can find an example of how to instrument a Scala application running Apache Spark jobs at this link:  
[link to example application](../examples/testSparkMeasureScala)
 
How to run the example:
 ```
# build the jar
sbt package

bin/spark-submit --master local[*] --packages ch.cern.sparkmeasure:spark-measure_2.12:0.19 --class ch.cern.testSparkMeasure.testSparkMeasure <path>/testsparkmeasurescala_2.12-0.1.jar
 ```
 
### Collect and save Stage Metrics
An example of how to collect task metrics aggregated at the stage execution level.
Some relevant snippet of code are:
 ```scala
     val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)
     stageMetrics.runAndMeasure {
       spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
     }
 
     // print report to standard output
     stageMetrics.printReport()

     // New in sparkMeasure v0.21, memory metrics report:
     stageMetrics.printMemoryReport()
 
     //save session metrics data
     val df = stageMetrics.createStageMetricsDF("PerfStageMetrics")
     stageMetrics.saveData(df.orderBy("jobId", "stageId"), "/tmp/stagemetrics_test1")
 
     val aggregatedDF = stageMetrics.aggregateStageMetrics("PerfStageMetrics")
     stageMetrics.saveData(aggregatedDF, "/tmp/stagemetrics_report_test2")
```

### Task metrics
Collecting Spark task metrics at the granularity of each task completion has additional overhead
compare to collecting at the stage completion level, therefore this option should only be used if you need data with this finer granularity, for example because you want
to study skew effects, otherwise consider using stagemetrics aggregation as preferred choice.

- The API for collecting data at task level is similar to the stage metrics case.
  An example:
    ```scala
    val taskMetrics = ch.cern.sparkmeasure.TaskMetrics(spark)
    taskMetrics.runAndMeasure {
      spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
    }
    ```

### Export to Prometheus PushGateway

You have the option to export aggregated stage metrics and/or task metrics a Prometheus push gateway.
See details at: [Prometheus Pushgateway](Prometheus.md)

### Run sparkMeasure using the packaged version from Maven Central

- This is how to run sparkMeasure using a packaged version in Maven Central
    ```
    bin/spark-submit --packages ch.cern.sparkmeasure:spark-measure_2.12:0.19

    // or just download and use the jar (it is only needed in the driver) as in:
    bin/spark-submit --conf spark.driver.extraClassPath=<path>/spark-measure_2.12-0.19.jar ...
   ```
- The alternative, see paragraph above, is to build a jar from master (See below).

### Download and build sparkMeasure (optional)

- If you want to build from the latest development version:
   ```
   git clone https://github.com/lucacanali/sparkmeasure
   cd sparkmeasure
   sbt +package
   ls -l target/scala-2.12/spark-measure*.jar  # location of the compiled jar

   # Run as in one of these examples:
   bin/spark-submit --jars path>/spark-measure_2.12-0.20-SNAPSHOT.jar
   
   # alternative, set classpath for the driver (it is only needed in the driver)
   bin/spark-submit --conf spark.driver.extraClassPath=<path>/spark-measure_2.12-0.20-SNAPSHOT.jar ...
   ```
