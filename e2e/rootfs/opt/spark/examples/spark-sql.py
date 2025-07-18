"""
A simple example demonstrating the use of sparkMeasure to instrument Python code running Apache Spark workloads
also expose the metrics to a prometheus exporter
"""

import logging

import time
from pyspark.sql import SparkSession
from sparkmeasure import StageMetrics
from sparkmeasure.jmx import jmxexport

def run_my_workload(spark):

    stagemetrics = StageMetrics(spark)

    stagemetrics.begin()
    spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
    stagemetrics.end()

    # print report to standard output
    stagemetrics.print_report()

    # get metrics data as a dictionary
    current_metrics = stagemetrics.aggregate_stagemetrics()
    print(f"metrics elapsedTime = {current_metrics.get('elapsedTime')}")

    # export metrics to JMX Prometheus exporter
    jmxexport(spark, current_metrics)

    # save session metrics data in json format (default)
    df = stagemetrics.create_stagemetrics_DF("PerfStageMetrics")
    stagemetrics.save_data(df.orderBy("jobId", "stageId"), "/tmp/stagemetrics_test1")

    aggregatedDF = stagemetrics.aggregate_stagemetrics_DF("PerfStageMetrics")
    stagemetrics.save_data(aggregatedDF, "/tmp/stagemetrics_report_test2")

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    # The Spark session is expected to be already up, created by spark-submit,
    # which handles also adding the sparkmeasure jar we just need to get a reference to it
    spark = (SparkSession
             .builder
             .appName("Test sparkmeasure instrumentation of Python/PySpark code")
             .getOrCreate()
            )
    # run Spark workload with instrumentation
    run_my_workload(spark)

    # Wait for a while to allow metrics to be collected and exported
    # This is just for demonstration purposes, in a real application you would not need to wait
    logging.info("Waiting for 1 hour to allow metrics to be collected and exported...")
    time.sleep(3600)

    spark.stop()
