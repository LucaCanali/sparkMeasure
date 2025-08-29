package ch.cern.testSparkMeasure

import org.apache.spark.sql._

import ch.cern.sparkmeasure.{StageMetrics, TaskMetrics}

/**
  * Test sparkMeasure (https://github.com/LucaCanali/sparkMeasure). Use:
  * bin/spark-submit --packages ch.cern.sparkmeasure:spark-measure_2.13:0.26 \
  * --class ch.cern.testSparkMeasure.testSparkMeasure <path>/testsparkmeasurescala_2.13-0.1.jar
  */
object testSparkMeasure {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("testSparkMeasure").
      getOrCreate()

    val stageMetrics = StageMetrics(spark)
    stageMetrics.runAndMeasure {
      spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
    }

    // print report to standard output
    stageMetrics.printReport()

    // return the metrics as a Scala map
    val collectedStageMetrics = stageMetrics.aggregateStageMetrics()
    println(s"Metric elapsed time = ${collectedStageMetrics("elapsedTime")}")

    // save session metrics data
    val df = stageMetrics.createStageMetricsDF("PerfStageMetrics")
    stageMetrics.saveData(df.orderBy("jobId", "stageId"), "/tmp/stagemetrics_test1")

    val aggregatedDF = stageMetrics.aggregateStageMetrics("PerfStageMetrics")
    stageMetrics.saveData(aggregatedDF, "/tmp/stagemetrics_report_test2")

    // If you want to collect data at task completion level granularity, use taskMetrics as in
    val taskMetrics = ch.cern.sparkmeasure.TaskMetrics(spark)
    taskMetrics.runAndMeasure {
      spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
    }
    taskMetrics.printReport()
    val collectedTaskMetrics = taskMetrics.aggregateTaskMetrics()
    println(s"Metric number of tasks = ${collectedTaskMetrics("numTasks")}")

    spark.stop()
  }
}
