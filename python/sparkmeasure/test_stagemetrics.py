# Test for sparkmeasure/stagemetrics.py
from . import StageMetrics
from .testutils import setup_sparksession

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
