## Python: sparkMeasure on PySpark

Notes on how to use sparkMeasure to collect Spark workload metrics when using PySpark from command line 
or from a Jupyter notebook.  
See also [README](../README.md) for an introduction to sparkMeasure and its architecture.

### Download and build sparkMeasure

1. Download the github repo
    ```python
    git clone https://github.com/lucacanali/sparkmeasure
    ```
2. Install the Python wrapper package
    ```python
    cd sparkmeasure/python
    pip install .
    ```
3.
 - Deploy sparkMeasure from Maven Central (the alternative, see 3b below, is to build a jar from master)
    ```python
    bin/pyspark --packages ch.cern.sparkmeasure:spark-measure_2.11:0.11
    ```
 - Optionally build sparkMeasure jar (the alternative is to use a released varsion available on Maven Central).
    ```python
    cd <sparkmeasure home dir>
    sbt package
    ls -l target/scala-2.11/spark-measure*.jar  # location of the compiled jar
 
    # Run as in one of these examples:
    bin/pyspark --jars path>/spark-measure_2.11-0.12-SNAPSHOT.jar
    
    #alternative:
    bin/pyspark --conf spark.driver.extraClassPath=<path>/spark-measure_2.11-0.12-SNAPSHOT.jar
    ```
   
   
### Collect and print stage metrics
1. How to collect and print Spark task stage metrics using sparkMeasure, example in Python:
    ```python
    from sparkmeasure import StageMetrics
    stagemetrics = StageMetrics(spark)
   
    stagemetrics.begin()
    spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
    stagemetrics.end()
    stagemetrics.print_report()
    # optionally run also this:
    stagemetrics.print_accumulables()
   ```
2. Similar to example 1, but with a shortcut/workaround to run code and measure it with one command line:
    ```python
    from sparkmeasure import StageMetrics
    stagemetrics = StageMetrics(spark)
    
    stagemetrics.runandmeasure(locals(),
    'spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()')
    # optionally run also this:
    stagemetrics.print_accumulables() 
   ```

---
### Task metrics

This type of metrics collection granularity is more performance-heavy, as data from each task is 
collected before aggregation.
It should only be used if you need data with this finer granularity, for example because you want
to study skew effects, otherwise consider using stagemetrics aggregation as preferred choice.

- The API for collecting data at task level is similar to the stage metrics case.
  An example:
    ```python
    from sparkmeasure import TaskMetrics
    taskmetrics = TaskMetrics(spark)
    taskmetrics.runandmeasure(locals(),
    'spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()')
    ```
