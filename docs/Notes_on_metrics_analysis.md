## Notes on analysis of Spark performance metrics collected using sparkMeasure

One of the key features of sparkMeasure is that it makes data easily accessible for analysis.  
This is achieved by exporting the collected data into Spark DataFrames where they can be queries with Spark APIs and/or SQL.
In addition ,the metrics can be used for plotting and other visualizations, for example using Jupyter notebooks.

Example of analysis of Task Metrics using a Jupyter notebook at: [SparkTaskMetricsAnalysisExample.ipynb](../examples/SparkTaskMetricsAnalysisExample.ipynb)

Additional example code:
```scala
// export task metrics collected by the Listener into a DataFrame and registers as a temporary view 
val df = taskMetrics.createTaskMetricsDF("PerfTaskMetrics")

// other option: read metrics previously saved on a json file
val df = spark.read.json("taskmetrics_test1")
df.createOrReplaceTempView("PerfTaskMetrics")

// show the top 5 tasks by duration
spark.sql("select jobId, host, duration from PerfTaskMetrics order by duration desc limit 5").show()
// show the available metrics
spark.sql("desc PerfTaskMetrics").show()
```
