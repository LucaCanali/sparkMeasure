# Test for sparkmeasure/stagemetrics.py

# Note this requires pytest and pyspark to be installed
from pyspark.sql import SparkSession
from . import StageMetrics
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
    stagemetrics = StageMetrics(spark)

    stagemetrics.begin()
    spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
    stagemetrics.end()

    # print report to standard output
    stagemetrics.print_report()

    # get metrics data as a dictionary
    metrics = stagemetrics.aggregate_stagemetrics()
    print(f"metrics elapsedTime = {metrics.get('elapsedTime')}")
    assert metrics.get('elapsedTime') > 0

    # map metrics into Spark DataFrames
    df = stagemetrics.create_stagemetrics_DF("PerfStageMetrics")
    aggregatedDF = stagemetrics.aggregate_stagemetrics_DF("PerfStageMetrics")

    assert df.count() > 0
    assert aggregatedDF.count() > 0

    spark.stop()
