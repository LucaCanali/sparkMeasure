# Note this requires pytest and pyspark to be installed
import pytest
from pyspark.sql import SparkSession
import glob
import os

@pytest.fixture
def setup_sparksession():
    # Note this is supposed to run after sbt package on the sparkmeasure project
    # so that we can get the jar file from the target folder
    SPARKMEASURE_JAR_FOLDER = "target/scala-2.12/"

    # Scan SPARKMEASURE_JAR_FOLDER for the spark-measure jar file
    # Set the directory you want to scan
    pattern = 'spark-measure_*.jar'  # Change this to your desired pattern

    # Use glob to find files matching the pattern
    files = glob.glob(os.path.join(SPARKMEASURE_JAR_FOLDER, pattern))

    if len(files) == 0:
        raise FileNotFoundError(f"No files matching pattern '{pattern}' found in {SPARKMEASURE_JAR_FOLDER}")
    elif len(files) > 1:
        raise FileExistsError(f"Multiple files matching pattern '{pattern}' found in {SPARKMEASURE_JAR_FOLDER}: {files}")

    jarfile=files[0]

    spark = (SparkSession.builder
            .appName("Test sparkmeasure instrumentation of Python/PySpark code")
            .master("local[*]")
            .config("spark.jars", jarfile)
            .getOrCreate()
            )
    return spark