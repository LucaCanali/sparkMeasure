"""
A simple example demonstrating the use of sparkMeasure to instrument Python code running Apache Spark workloads
Prerequisite:
   pip install sparkmeasure
Run with:
  ./bin/spark-submit --packages ch.cern.sparkmeasure:spark-measure_2.12:0.23 test_sparkmeasure_python.py
"""

from pyspark.sql import SparkSession
from sparkmeasure import StageMetrics


def run_my_workload(spark):
    stagemetrics = StageMetrics(spark)

    stagemetrics.begin()
    spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
    stagemetrics.end()

    # print report to standard output
    stagemetrics.print_report()

    # get metrics data as a dictionary
    metrics = stagemetrics.aggregate_stagemetrics()
    print(f"metrics elapsedTime = {metrics.get('elapsedTime')}")

    # save session metrics data in json format (default)
    df = stagemetrics.create_stagemetrics_DF("PerfStageMetrics")
    stagemetrics.save_data(df.orderBy("jobId", "stageId"), "/tmp/stagemetrics_test1")

    aggregatedDF = stagemetrics.aggregate_stagemetrics_DF("PerfStageMetrics")
    stagemetrics.save_data(aggregatedDF, "/tmp/stagemetrics_report_test2")


if __name__ == "__main__":
    # The Spark session is expected to be already up, created by spark-submit,
    # which handles also adding the sparkmeasure jar we just need to get a reference to it
    spark = (SparkSession
             .builder
             .appName("Test sparkmeasure instrumentation of Python/PySpark code")
             .getOrCreate()
            )
    # run Spark workload with instrumentation
    run_my_workload(spark)
    spark.stop()
