"""sparkMeasure is a tool for performance troubleshooting of Apache Spark workloads. It simplifies the collection and analysis of Spark task metrics.
"""

from __future__ import print_function

"""StageMetrics class is the API to collect and process task metrics data aggregated by stage
This is a Python wrapper class to the corresponding Scala class of sparkMeasure
"""
class StageMetrics:
    def __init__(self,sparksession):
        self.sparksession = sparksession
        self.sc = self.sparksession.sparkContext
        self.stagemetrics = self.sc._jvm.ch.cern.sparkmeasure.StageMetrics(self.sparksession._jsparkSession)

    def begin(self):
        self.stagemetrics.begin()

    def end(self):
        self.stagemetrics.end()

    def printreport(self):
        print(self.stagemetrics.report())

    def runandmeasure(self, codetorun):
        stagemetrics.begin()
        exec(codetorun)
        stagemetrics.end()
        stagemetrics.printreport()


