"""sparkmeasure is a tool for performance troubleshooting of Apache Spark workloads.
It simplifies the collection and analysis of Spark task metrics data.
See details at https://github.com/LucaCanali/sparkMeasure
"""

from __future__ import print_function

"""StageMetrics class provides the API to collect and process task metrics data aggregated by execution stage.
This is a Python wrapper class to the corresponding Scala class of sparkMeasure.
"""
class StageMetrics:
    def __init__(self, sparksession):
        self.sparksession = sparksession
        self.sc = self.sparksession.sparkContext
        self.stagemetrics = self.sc._jvm.ch.cern.sparkmeasure.StageMetrics(self.sparksession._jsparkSession)

    def begin(self):
        self.stagemetrics.begin()

    def end(self):
        self.stagemetrics.end()

    def report(self):
        return self.stagemetrics.report()

    def report_accumulables(self):
        return self.stagemetrics.reportAccumulables()

    def print_report(self):
        print(self.report())

    def print_accumulables(self):
        print(self.report_accumulables())

    def runandmeasure(self, env, codetorun):
        self.begin()
        exec codetorun in env
        self.end()
        self.print_report()

    def create_stagemetrics_DF(self, viewname):
        df = self.stagemetrics.createStageMetricsDF(viewname)
        # convert the returned Java object to a Python Dataframe
        from pyspark.sql.dataframe import DataFrame
        return DataFrame(df, self.sparksession)

    def aggregate_stagemetrics_DF(self, viewname):
        df = self.stagemetrics.aggregateStageMetrics(viewname)
        # convert the returned Java object to a Python Dataframe
        from pyspark.sql.dataframe import DataFrame
        return DataFrame(df, self.sparksession)

    def save_data(self, df, filepathandname, fileformat="json"):
        df.repartition(1).write.format(fileformat).save(filepathandname)
