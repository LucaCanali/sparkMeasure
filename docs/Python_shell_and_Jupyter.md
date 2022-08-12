# sparkMeasure on PySpark

Notes on how to use sparkMeasure to collect Spark workload metrics when using PySpark from command line 
or from a Jupyter notebook.  
See also [README](../README.md) for an introduction to sparkMeasure and its architecture.

### Deployment and installation

- Use PyPi to install the Python wrapper and take the jar from Maven central: 
    ```
    pip install sparkmeasure
    bin/pyspark --packages ch.cern.sparkmeasure:spark-measure_2.12:0.19
    ```
 - If you prefer to build from the latest development version:
    ```
    git clone https://github.com/lucacanali/sparkmeasure
    cd sparkmeasure
    sbt +package
    ls -l target/scala-2.12/spark-measure*.jar  # note location of the compiled and packaged jar
 
    # Install the Python wrapper package
    cd python
    pip install .
    
    # Run as in one of these examples:
    bin/pyspark --jars path>/spark-measure_2.12-0.20-SNAPSHOT.jar
    
    #alternative:
    bin/pyspark --conf spark.driver.extraClassPath=<path>/spark-measure_2.12-0.20-SNAPSHOT.jar
    ```
   
   
### PySpark example
1. How to collect and print Spark task stage metrics using sparkMeasure, example in Python:
    ```python
    from sparkmeasure import StageMetrics
    stagemetrics = StageMetrics(spark)
   
    stagemetrics.begin()
    spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
    stagemetrics.end()

    stagemetrics.print_report()
    stageMetrics.print_memory_report()
   ```
2. Similar to example 1, but with a shortcut to run code and measure it with one command line:
    ```python
    from sparkmeasure import StageMetrics
    stagemetrics = StageMetrics(spark)
    
    stagemetrics.runandmeasure(globals(),
    'spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()')
   
   stageMetrics.print_memory_report()
   ```

### Jupyter notebook example

Jupyter notebooks are a popular way to interact with PySpark for data analysis.  
Example Jupyter notebook showing the use of basic sparkMeasure instrumentation:
  
[SparkMeasure_Jupyer_Python_getting_started.ipynb](examples/SparkMeasure_Jupyer_Python_getting_started.ipynb)

Note, in particular with Jupyter notebooks it can be handy to write cell magic to wrap the instrumentation,
as in the following code (see notebook above for details):
```python
# Define cell and line magic to wrap the instrumentation
from IPython.core.magic import (register_line_magic, register_cell_magic, register_line_cell_magic)

@register_line_cell_magic
def sparkmeasure(line, cell=None):
    "run and measure spark workload. Use: %sparkmeasure or %%sparkmeasure"
    val = cell if cell is not None else line
    stagemetrics.begin()
    eval(val)
    stagemetrics.end()
    stagemetrics.print_report()
```

### Collecting metrics at finer granularity: use Task metrics

Collecting Spark task metrics at the granularity of each task completion has additional overhead
compare to collecting at the stage completion level, therefore this option should only be used if you need data with 
this finer granularity, for example because you want to study skew effects, otherwise consider using
stagemetrics aggregation as preferred choice.

- The API for collecting data at task level is similar to the stage metrics case.
  Examples:
    ```python
    from sparkmeasure import TaskMetrics
    taskmetrics = TaskMetrics(spark)
    taskmetrics.begin()
    spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
    taskmetrics.end()
    taskmetrics.print_report()
    ```
  
    ```python
    from sparkmeasure import TaskMetrics
    taskmetrics = TaskMetrics(spark)
    taskmetrics.runandmeasure(globals(),
    'spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()')
    ```

### Exporting metrics data for archiving and/or further analysis

One simple use case is to make use of the data collected and reported by stagemetrics and taskmetrics 
printReport methods for immediate troubleshooting and workload analysis.  
You also have options to save metrics aggregated as in the printReport output.  

Another option is to export the metrics to an external system, see details at [Prometheus Pushgateway](Prometheus.md) 
  
- Example on how to export raw Stage metrics data in json format
    ```python
    from sparkmeasure import StageMetrics
    stagemetrics = StageMetrics(spark)
    stagemetrics.runandmeasure(globals(), ...your workload here ... )
  
    df = stagemetrics.create_stagemetrics_DF("PerfStageMetrics")
    df.show()
    stagemetrics.save_data(df.orderBy("jobId", "stageId"), "stagemetrics_test1", "json")
    ```

- Example, save aggregated metrics (as found in the printReport output) in json format

    ```python
    stageMetrics.create_stagemetrics_DF("PerfStageMetrics")
    aggregatedDF = stagemetrics.aggregate_stagemetrics_DF("PerfStageMetrics")
    aggregatedDF.show()
    stagemetrics.save_data(aggregatedDF, "stagemetrics_report_test2", "json")
    ```

- Example on how to export raw Task Metrics data in json format
    ```python
    df = taskMetrics.create_taskmetrics_DF("PerfTaskMetrics")
    spark.sql("select * from PerfTaskMetrics").show()
    show()
    taskMetrics.save_data(df.orderBy("jobId", "stageId", "index"), "taskmetrics_test3", "json")
    ```

- New in sparkMeasure v01: memory metrics report:
```
stageMetrics.print_memory_report()

Additional stage-level executor metrics (memory usage info):

Stage 0 JVMHeapMemory maxVal bytes => 279558120 (266.6 MB)
Stage 0 OnHeapExecutionMemory maxVal bytes => 0 (0 Bytes)
Stage 1 JVMHeapMemory maxVal bytes => 279558120 (266.6 MB)
Stage 1 OnHeapExecutionMemory maxVal bytes => 0 (0 Bytes)
Stage 3 JVMHeapMemory maxVal bytes => 279558120 (266.6 MB)
Stage 3 OnHeapExecutionMemory maxVal bytes => 0 (0 Bytes)
```

    