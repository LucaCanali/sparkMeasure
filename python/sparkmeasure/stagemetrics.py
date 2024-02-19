"""sparkmeasure is a tool for performance troubleshooting of Apache Spark workloads.
It simplifies the collection and analysis of Spark task metrics data.
See details at https://github.com/LucaCanali/sparkMeasure
"""


class StageMetrics:
    """StageMetrics class provides the API to collect and process task metrics data aggregated by execution stage.
       This is a Python wrapper class to the corresponding Scala class of sparkMeasure."""
    def __init__(self, sparksession):
        self.sparksession = sparksession
        self.sc = self.sparksession.sparkContext
        try:
            self.stagemetrics = self.sc._jvm.ch.cern.sparkmeasure.StageMetrics(self.sparksession._jsparkSession)
        except Exception as e:
            # Handle the case where the sparkmeasure jar is not found
            print("The class ch.cern.sparkmeasure.StageMetrics does not exist or could not be loaded.", e)
            print("\nError: the sparkMeasure jar is like not loaded")
            print("Please check configuration: spark.jars.packages, spark.jars and/or driver classpath configuration and re-run the workload")

    def begin(self):
        """Begin collecting stage metrics data."""
        self.stagemetrics.begin()

    def end(self):
        """End collecting stage metrics data."""
        self.stagemetrics.end()

    def report(self):
        """Return a report of the collected stage metrics data."""
        return self.stagemetrics.report()

    def aggregate_stagemetrics(self):
        """Return a dictionary of the aggregated stage metrics data."""
        return self.stagemetrics.aggregateStageMetricsJavaMap()

    def print_report(self):
        """Print a report of the collected stage metrics data."""
        print(self.report())

    def report_memory(self):
        """Return the collected stage metrics data for memory usage."""
        return self.stagemetrics.reportMemory()

    def print_memory_report(self):
        """Print a report of the collected stage metrics data for memory usage."""
        print(self.report_memory())

    def runandmeasure(self, env, codetorun):
        """Run a Python code snippet and collect stage metrics data."""
        self.begin()
        exec(codetorun, env)
        self.end()
        self.print_report()

    def create_stagemetrics_DF(self, viewname="PerfStageMetrics"):
        """Create a Spark Dataframe from the collected stage metrics data."""
        df = self.stagemetrics.createStageMetricsDF(viewname)
        # convert the returned Java object to a Python Dataframe
        from pyspark.sql.dataframe import DataFrame
        return DataFrame(df, self.sparksession)

    def aggregate_stagemetrics_DF(self, viewname="PerfStageMetrics"):
        """Create a Spark Dataframe from the aggregated stage metrics data."""
        df = self.stagemetrics.aggregateStageMetrics(viewname)
        # convert the returned Java object to a Python Dataframe
        from pyspark.sql.dataframe import DataFrame
        return DataFrame(df, self.sparksession)

    def save_data(self, df, filepathandname, fileformat="json"):
        """Save the collected stage metrics data to a file."""
        df.repartition(1).write.format(fileformat).save(filepathandname)

    def remove_listener(self):
        """Remove the Spark listener that collects stage metrics data."""
        self.stagemetrics.removeListener()
