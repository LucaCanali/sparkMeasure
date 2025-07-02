# Test for sparkmeasure/jmx.py
import re

# Note this requires pytest and pyspark to be installed
from . import StageMetrics
from .jmx import jmxexport
from .testutils import setup_sparksession

def test_jmxexport(setup_sparksession):
    spark = setup_sparksession
    conf = spark.sparkContext.getConf()
    conf.set("spark.sql.shuffle.partitions", "2")
    conf.set("spark.default.parallelism", "2")
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

    jmxexport(spark, metrics)

    dropwizard = spark._jvm.ch.cern.metrics.DropwizardMetrics

    # Regex pour matcher la sortie attendue avec une étoile pour resultSize
    expected_regex = (
        r"Metrics for namespace 'unknown' and pod 'unknown'\n"
        r"Total gauges: 2\n"
        r"Gauge: unknown\.unknown\.resultSize = [0-9]+(\.[0-9]+)?\n"
        r"Gauge: unknown\.unknown\.peakExecutionMemory = 0\.0\n"
        r"Total counters: 19\n"
        r"Counter: unknown\.unknown\.bytesRead = 0\n"
        r"Counter: unknown\.unknown\.numStages = 3\n"
        r"Counter: unknown\.unknown\.shuffleRecordsWritten = [0-9]+\n"
        r"Counter: unknown\.unknown\.shuffleRemoteBytesRead = 0\n"
        r"Counter: unknown\.unknown\.shuffleLocalBlocksFetched = [0-9]+\n"
        r"Counter: unknown\.unknown\.shuffleTotalBlocksFetched = [0-9]+\n"
        r"Counter: unknown\.unknown\.memoryBytesSpilled = 0\n"
        r"Counter: unknown\.unknown\.bytesWritten = 0\n"
        r"Counter: unknown\.unknown\.numTasks = [0-9]+\n"
        r"Counter: unknown\.unknown\.recordsWritten = 0\n"
        r"Counter: unknown\.unknown\.shuffleRecordsRead = [0-9]+\n"
        r"Counter: unknown\.unknown\.recordsRead = 2000\n"
        r"Counter: unknown\.unknown\.shuffleLocalBytesRead = [0-9]+\n"
        r"Counter: unknown\.unknown\.shuffleBytesWritten = [0-9]+\n"
        r"Counter: unknown\.unknown\.shuffleTotalBytesRead = [0-9]+\n"
        r"Counter: unknown\.unknown\.metrics_published_total = 30\n"
        r"Counter: unknown\.unknown\.diskBytesSpilled = 0\n"
        r"Counter: unknown\.unknown\.shuffleRemoteBytesReadToDisk = 0\n"
        r"Counter: unknown\.unknown\.shuffleRemoteBlocksFetched = 0\n"
        r"Total timers: 10\n"
        r"Timer: unknown.unknown.shuffleWriteTime = count=1\n"
        r"Timer: unknown.unknown.stageDuration = count=1\n"
        r"Timer: unknown.unknown.executorCpuTime = count=1\n"
        r"Timer: unknown.unknown.shuffleFetchWaitTime = count=1\n"
        r"Timer: unknown.unknown.executorRunTime = count=1\n"
        r"Timer: unknown.unknown.jvmGCTime = count=1\n"
        r"Timer: unknown.unknown.elapsedTime = count=1\n"
        r"Timer: unknown.unknown.executorDeserializeCpuTime = count=1\n"
        r"Timer: unknown.unknown.resultSerializationTime = count=1\n"
        r"Timer: unknown.unknown.executorDeserializeTime = count=1\n"
    )

    output = dropwizard.describeMetrics()
    print(output)
    assert re.search(expected_regex, output, re.MULTILINE | re.DOTALL), f"Output does not match expected regex.\nGot:\n{output}\nWant:\n{expected_regex}"

    spark.stop()
