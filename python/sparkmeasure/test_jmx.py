# Test for sparkmeasure/jmx.py
import re

# Note this requires pytest and pyspark to be installed
from . import StageMetrics
from .jmx import jmxexport
from .testutils import setup_sparksession


LINE_END = r"(?:\r?\n|$)"
NUM = r"[0-9]+(?:\.[0-9]+)?"  # integer or float


def _must_match(pattern: str, text: str, msg: str):
    """Assert that regex pattern matches text; show diagnostic output on failure."""
    m = re.search(pattern, text, re.MULTILINE | re.DOTALL)
    assert m, f"{msg}\n--- Pattern ---\n{pattern}\n--- Output ---\n{text}"
    return m


def test_jmxexport(setup_sparksession):
    spark = setup_sparksession

    # Keep small, deterministic-ish plan for CI
    conf = spark.sparkContext.getConf()
    conf.set("spark.sql.shuffle.partitions", "2")
    conf.set("spark.default.parallelism", "2")

    stagemetrics = StageMetrics(spark)
    stagemetrics.begin()
    # Non-trivial job to produce meaningful metrics
    spark.sql(
        "select count(*) from range(1000) cross join range(1000) cross join range(1000)"
    ).show()
    stagemetrics.end()

    # Report (useful in CI logs)
    stagemetrics.print_report()

    # Basic sanity on aggregated metrics
    metrics = stagemetrics.aggregate_stagemetrics()
    assert metrics.get("elapsedTime", 0) > 0, "elapsedTime should be > 0"

    # Export via Dropwizard/JMX and capture string description
    jmxexport(spark, metrics)
    dropwizard = spark._jvm.ch.cern.metrics.DropwizardMetrics
    output = dropwizard.describeMetrics()
    print(output)  # leave for CI diagnostics

    # 1) Header: capture namespace/pod (can be 'unknown' or a real name)
    header_pat = (
        r"Metrics for namespace '([A-Za-z0-9._-]+)' and pod '([A-Za-z0-9._-]+)'" + LINE_END
    )
    m = _must_match(header_pat, output, "Missing or malformed header with namespace/pod")
    ns, pod = m.group(1), m.group(2)

    # 2) Totals present (counts may vary)
    _must_match(r"Total gauges: \d+" + LINE_END, output, "Missing 'Total gauges' line")
    _must_match(r"Total counters: \d+" + LINE_END, output, "Missing 'Total counters' line")
    _must_match(r"Total timers: \d+" + LINE_END, output, "Missing 'Total timers' line")

    # 3) Gauges: order may vary, so assert independently
    gauge_prefix = rf"Gauge: {re.escape(ns)}\.{re.escape(pod)}\."
    _must_match(
        gauge_prefix + rf"resultSize = {NUM}" + LINE_END,
        output,
        "Missing 'resultSize' gauge",
    )
    _must_match(
        gauge_prefix + rf"peakExecutionMemory = {NUM}" + LINE_END,
        output,
        "Missing 'peakExecutionMemory' gauge",
    )

    # 4) Counters: assert presence (values vary by env)
    counters = [
        "bytesRead",
        "bytesWritten",
        "numStages",
        "numTasks",
        "recordsRead",
        "recordsWritten",
        "shuffleRecordsRead",
        "shuffleRecordsWritten",
        "shuffleLocalBlocksFetched",
        "shuffleTotalBlocksFetched",
        "shuffleLocalBytesRead",
        "shuffleBytesWritten",
        "shuffleTotalBytesRead",
        "memoryBytesSpilled",
        "diskBytesSpilled",
        "shuffleRemoteBlocksFetched",
        "shuffleRemoteBytesRead",
        "shuffleRemoteBytesReadToDisk",
        "metrics_published_total",
    ]
    for cname in counters:
        _must_match(
            rf"Counter: {re.escape(ns)}\.{re.escape(pod)}\.{re.escape(cname)} = \d+"
            + LINE_END,
            output,
            f"Missing counter '{cname}'",
        )

    # 5) Timers: ensure core timers exist; counts may vary
    timers = [
        "elapsedTime",
        "stageDuration",
        "executorRunTime",
        "executorCpuTime",
        "executorDeserializeTime",
        "executorDeserializeCpuTime",
        "resultSerializationTime",
        "jvmGCTime",
        "shuffleWriteTime",
        "shuffleFetchWaitTime",
    ]
    for tname in timers:
        _must_match(
            rf"Timer: {re.escape(ns)}\.{re.escape(pod)}\.{re.escape(tname)} = count=\d+"
            + LINE_END,
            output,
            f"Missing timer '{tname}'",
        )

    spark.stop()

