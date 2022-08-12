# Use sparkMeasure to instrument Python/PySpark code
  
  SparkMeasure can be used to instrument your Python code to measure Apache Spark workload.
  Use this for example for performance troubleshooting, application instrumentation, workload studies, etc.
  
 
### Example code 

You can find an example of how to instrument a Scala application running Apache Spark jobs at this link:  
 [link to example Python application](../examples/test_sparkmeasure_python.py)
 
How to run the example:
 ```
./bin/spark-submit --packages ch.cern.sparkmeasure:spark-measure_2.12:0.19 test_sparkmeasure_python.py
 ```

 Some relevant snippet of code are:
 ```python
    from sparkmeasure import StageMetrics
    stagemetrics = StageMetrics(spark)

    stagemetrics.begin()
    spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
    stagemetrics.end()

    # print report to standard output
    stagemetrics.print_report()
    
    # New in sparkMeasure v0.21, memory metrics report:
    stageMetrics.print_memory_report()

    # save session metrics data in json format (default)
    df = stagemetrics.create_stagemetrics_DF("PerfStageMetrics")
    stagemetrics.save_data(df.orderBy("jobId", "stageId"), "/tmp/stagemetrics_test1")

    aggregatedDF = stagemetrics.aggregate_stagemetrics_DF("PerfStageMetrics")
    stagemetrics.save_data(aggregatedDF, "/tmp/stagemetrics_report_test2")
```

Note: if you want to collect metrics at the task execution level, you can use TaskMetrics instead of StamgeMetrics.
The details are discussed in the [examples for Python shell and notebook](docs/Python_shell_and_Jupyter.md).

### Run sparkMeasure using the packaged version from Maven Central

- This is how to run sparkMeasure using a packaged version in Maven Central
  ```
  bin/spark-submit --packages ch.cern.sparkmeasure:spark-measure_2.12:0.19 your_python_code.py

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

     cd python
     pip install .
  
     # Run as in one of these examples:
     bin/spark-submit --jars path>/spark-measure_2.12-0.20-SNAPSHOT.jar ...
     
     # alternative, set classpath for the driver (sparkmeasure code runs only in the driver)
     bin/spark-submit --conf spark.driver.extraClassPath=<path>/spark-measure_2.12-0.20-SNAPSHOT.jar ...
     ```
