"""sparkmeasure is a tool for performance troubleshooting of Apache Spark workloads.
It simplifies the collection and analysis of Spark task metrics data.
See details at https://github.com/LucaCanali/sparkMeasure
"""

from __future__ import print_function

"""TaskMetrics class provides the API to collect and process task metrics data aggregated by task execution
This is a finer granularity than StageMetrics and potentially collects much more data.
This is a Python wrapper class to the corresponding Scala class of sparkMeasure.
"""
class TaskMetrics:
    def __init__(self, sparksession):
        self.sparksession = sparksession
        self.sc = self.sparksession.sparkContext
        self.taskmetrics = self.sc._jvm.ch.cern.sparkmeasure.TaskMetrics(self.sparksession._jsparkSession, False)

    def begin(self):
        self.taskmetrics.begin()

    def end(self):
        self.taskmetrics.end()

    def report(self):
        return self.taskmetrics.report()

    def print_report(self):
        print(self.report())

    def print_accumulables(self):
        none # not yet implemented

    def runandmeasure(self, env, codetorun):
        self.begin()
        exec codetorun in env
        self.end()
        self.print_report()

    def create_taskmetrics_DF(self, viewname):
        self.taskmetrics.createStageMetricsDF(viewname)

    def save_data(self, df, filepathandname, fileformat):
        df.repartition(1).write.format(fileformat).save(filepathandname)
