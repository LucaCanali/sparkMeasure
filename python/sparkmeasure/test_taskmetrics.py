# Test for sparkmeasure/stagemetrics.py

# Note this requires pytest and pyspark to be installed
from pyspark.sql import SparkSession
from . import TaskMetrics
import pytest

@pytest.fixture
def setup_sparksession():
    # Note this is supposed to run after sbt package on the sparkmeasure project
    # so that we can get the jar file from the target folder
    SPARKMEASURE_JAR_FOLDER = "target/scala-2.12/"
    spark = (SparkSession.builder
            .appName("Test sparkmeasure instrumentation of Python/PySpark code")
            .master("local[*]")
            .config("spark.jars", SPARKMEASURE_JAR_FOLDER + "*.jar")
            .getOrCreate()
            )
    return spark

def test_stagemetrics(setup_sparksession):
    spark = setup_sparksession
    taskmetrics = TaskMetrics(spark)

    taskmetrics.begin()
    spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
    taskmetrics.end()

    # print report to standard output
    taskmetrics.print_report()

    # get metrics data as a dictionary
    metrics = taskmetrics.aggregate_taskmetrics()
    print(f"metrics elapsedTime = {metrics.get('elapsedTime')}")
    assert metrics.get('numTasks') > 0

    # map metrics into Spark DataFrames
    df = taskmetrics.create_taskmetrics_DF("PerfTaskMetrics")
    aggregatedDF = taskmetrics.aggregate_taskmetrics_DF("PerfTaskMetrics")

    assert df.count() > 0
    assert aggregatedDF.count() > 0

    spark.stop()
