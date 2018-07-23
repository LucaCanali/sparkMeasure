## A list of TODO tasks, ideas and known issues of sparkMeasure

Use this as a reference for future development


**Known issues and TODO list**
   * Task/stage failures and other errors are mostly not handled by the code in this version, this puts the effort
     on the user to validate the output. This needs to be fixed in a future version.
   * We can expected more task metrics being added in future versions. Current code is not version aware and does
     not offer and easy way to handle additional metrics only for newer versions without breaking backward compatibility.  
     TODO: implement Spark version awareness and custom list of metrics in sparkMeasure  
           + Following [SPARK PR 18249](https://github.com/apache/spark/pull/18249/files) add support for the metric 
     remoteBytesReadToDisk Task Metric (this is relevant for Spark 2.3.x and above).     
   * TODO: Flight recorder mode, task metrics, find ways to write metrics out to output files incrementally, 
     rather than using the current approach of buffering everything in memory and writing at the end? 
     The current approach has ovious scalability issues.
   * Following [SPARK PR 18162](https://github.com/apache/spark/pull/18162) TaskMetrics._updatedBlockStatuses 
     is off by default.  
     TODO (maybe): remove _updatedBlockStatuses from the list of metrics collected by spakMeasure
   * TODO (maybe) implement in sparmMeasure APIS removeSparkListener method, to allow stopping data collection 
     from sparkMeasure. (note this is only possible from Spark version 2.2 and above)
   * gatherAccumulables=true for taskMetrics(sparkSession: SparkSession, gatherAccumulables: Boolean) 
     currently only works on Spark 2.1.x and breaks from Spark 2.2.1. This is a consequence of
      [SPARK PR 17596](https://github.com/apache/spark/pull/17596).  
      TODO (maybe): restore the functionality of measuring task accumulables for Spark 2.2.x
   * TODO (maybe): ost-processing of metrics data in scala, rather than Spark SQL? 
     The advantage would be not to "pollute" the execution environment with additional SQL jobs as is the case now.
