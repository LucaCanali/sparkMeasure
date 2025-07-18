import sys
from random import random
from operator import add
from time import sleep

from sparkmeasure import StageMetrics
from sparkmeasure.jmx import jmxexport
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError

if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonPi")\
        .getOrCreate()

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * partitions

    def f(_: int) -> float:
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    stagemetrics = StageMetrics(spark)
    stagemetrics.begin()

    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    print("Pi is roughly %f" % (4.0 * count / n))

    stagemetrics.end()

    stagemetrics.print_report()

    try:
        stagemetrics.print_memory_report()
    except Py4JJavaError:
        print("Memory report failed, retrying (see https://github.com/LucaCanali/sparkMeasure/blob/master/README.md#examples-of-sparkmeasure-on-the-cli)")
        sleep(5)
        stagemetrics.print_memory_report()

    spark.stop()
