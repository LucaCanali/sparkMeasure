# Spark Measure 
A proof-of-concept tool for measuring Spark performance extending the SparkListener
This is based on using Spark Listeners and collecting metrics in a ListBuffer
The list buffer is then transofrmed into a DataFrame for analysis
See also metric reports between two time snapshots and save method to persist data on disk

- Stage Metrics: collects and aggregates metrics at the end of each stage
- Task Metrics: collects data at task granularity

Example usage:
- val stageMetrics = new ch.cern.sparkmeasure.StageMetrics(spark)
- stageMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show)
 
Created by Luca.Canali@cern.ch, March 2017
Developed and tested on Spark 2.1.0

