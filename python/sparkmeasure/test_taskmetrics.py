# Test for sparkmeasure/stagemetrics.py
from . import TaskMetrics
from .testutils import setup_sparksession


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
