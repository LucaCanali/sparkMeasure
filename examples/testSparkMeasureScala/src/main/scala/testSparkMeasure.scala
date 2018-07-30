package ch.cern.testSparkMeasure

import org.apache.spark.sql._

/**
  * Test sparkMeasure (https://github.com/LucaCanali/sparkMeasure). Use:
  * bin/spark-submit --master local[*] --packages ch.cern.sparkmeasure:spark-measure_2.11:0.13 \
  * --class ch.cern.testSparkMeasure.testSparkMeasure <path>/testsparkmeasurescala_2.11-0.1.jar
  */
object testSparkMeasure {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.
      builder().
      appName("testSparkMeasure").
      getOrCreate()

    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)
    stageMetrics.runAndMeasure {
      spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
    }

    // print report to standard output
    stageMetrics.printReport()

    //save session metrics data
    val df = stageMetrics.createStageMetricsDF("PerfStageMetrics")
    stageMetrics.saveData(df.orderBy("jobId", "stageId"), "/tmp/stagemetrics_test1")

    val aggregatedDF = stageMetrics.aggregateStageMetrics("PerfStageMetrics")
    stageMetrics.saveData(aggregatedDF, "/tmp/stagemetrics_report_test2")

    // You can also instrument your code as in the following example:
    //stageMetrics.begin()
    //spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").collect()
    //stageMetrics.end()
    //stageMetrics.printReport()
    //stageMetrics.printAccumulables()
    //savedata as detailed above..

    spark.stop()
  }
}
