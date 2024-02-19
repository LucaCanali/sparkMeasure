"""sparkmeasure is a tool for performance troubleshooting of Apache Spark workloads.
It simplifies the collection and analysis of Spark task metrics data.
See details at https://github.com/LucaCanali/sparkMeasure
"""


class TaskMetrics:
    """TaskMetrics class provides the API to collect and process task metrics data aggregated by task execution
       This is a finer granularity than StageMetrics and potentially collects much more data.
       This is a Python wrapper class to the corresponding Scala class of sparkMeasure."""
    def __init__(self, sparksession):
        self.sparksession = sparksession
        self.sc = self.sparksession.sparkContext
        try:
            self.taskmetrics = self.sc._jvm.ch.cern.sparkmeasure.TaskMetrics(self.sparksession._jsparkSession)
        except Exception as e:
            # Handle the case where the sparkmeasure jar is not found
            print("The class ch.cern.sparkmeasure.TaskMetrics does not exist or could not be loaded.", e)
            print("\nError: the sparkMeasure jar is like not loaded")
            print("Please check configuration: spark.jars.packages, spark.jars and/or driver classpath configuration and re-run the workload")

    def begin(self):
        """Begin collecting task metrics data."""
        self.taskmetrics.begin()

    def end(self):
        """End collecting task metrics data."""
        self.taskmetrics.end()

    def report(self):
        """Return a report of the collected task metrics data."""
        return self.taskmetrics.report()

    def aggregate_taskmetrics(self):
        """Return a dictionary of the aggregated task metrics data."""
        return self.taskmetrics.aggregateTaskMetricsJavaMap()

    def print_report(self):
        """Print a report of the collected task metrics data."""
        print(self.report())

    def runandmeasure(self, env, codetorun):
        """Run a Python code snippet and collect task metrics data."""
        self.begin()
        exec(codetorun, env)
        self.end()
        self.print_report()

    def create_taskmetrics_DF(self, viewname="PerfTaskMetrics"):
        """Create a Spark Dataframe from the collected task metrics data."""
        df = self.taskmetrics.createTaskMetricsDF(viewname)
        # convert the returned Java object to a Python Dataframe
        from pyspark.sql.dataframe import DataFrame
        return DataFrame(df, self.sparksession)

    def aggregate_taskmetrics_DF(self, viewname="PerfTaskMetrics"):
        """Create a Spark Dataframe from the aggregated task metrics data."""
        df = self.taskmetrics.aggregateTaskMetrics(viewname)
        # convert the returned Java object to a Python Dataframe
        from pyspark.sql.dataframe import DataFrame
        return DataFrame(df, self.sparksession)

    def save_data(self, df, filepathandname, fileformat):
        """Save the collected task metrics data to a file."""
        df.repartition(1).write.format(fileformat).save(filepathandname)

    def remove_listener(self):
        """Remove the listener from the SparkContext."""
        self.taskmetrics.removeListener()
