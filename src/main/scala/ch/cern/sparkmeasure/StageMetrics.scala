package ch.cern.sparkmeasure

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable.{ListBuffer, LinkedHashMap}
import scala.math.{min, max}

/**
 *  Stage Metrics: collects stage-level metrics with Stage granularity
 *                 and provides aggregation and reporting functions for the end-user
 *
 * Example usage for stage metrics:
 * val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)
 * stageMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show)
 *
 * The tool is based on using Spark Listeners as data source and collecting metrics in a ListBuffer of
 * a case class that encapsulates Spark task metrics.
 * The List Buffer is then transformed into a DataFrame for ease of reporting and analysis.
 *
 * Stage metrics are stored in memory and use to produce a report that aggregates resource consumption
 * they can also be consumed "raw" (transformed into a DataFrame and/or saved to a file)
 *
 */
case class StageMetrics(sparkSession: SparkSession) {

  lazy val logger = LoggerFactory.getLogger(this.getClass.getName)
  val stageInfoVerbose = Utils.parseExtraStageMetrics(sparkSession.sparkContext.getConf, logger)
  val executorMetricsNames = Utils.parseExecutorMetricsConfig(sparkSession.sparkContext.getConf, logger)

  // This inserts and starts the custom Spark Listener into the live Spark Context
  val listenerStage = new StageInfoRecorderListener(stageInfoVerbose, executorMetricsNames)
  registerListener(sparkSession, listenerStage)

  // Variables used to store the start and end time of the period of interest for the metrics report
  var beginSnapshot: Long = 0L
  var endSnapshot: Long = 0L

  // Marks the beginning of data collection
  def begin(): Long = {
    listenerStage.stageMetricsData.clear()    // clear previous data to reduce memory footprint
    beginSnapshot = System.currentTimeMillis()
    endSnapshot = beginSnapshot
    beginSnapshot
  }

  // Marks the end of data collection
  def end(): Long = {
    endSnapshot = System.currentTimeMillis()
    endSnapshot
  }

  // helper method to register the listener
  def registerListener(spark: SparkSession, listener: StageInfoRecorderListener): Unit = {
    spark.sparkContext.addSparkListener(listener)
  }

  // helper method to remove the listener
  def removeListener(): Unit = {
    sparkSession.sparkContext.removeSparkListener(listenerStage)
  }

  // Compute basic aggregation on the Stage metrics for the metrics report
  // also filter on the time boundaries for the report
  def aggregateStageMetrics() : LinkedHashMap[String, Long] = {

    val agg = Utils.zeroMetricsStage()
    var submissionTime = Long.MaxValue
    var completionTime = 0L

    for (metrics <- listenerStage.stageMetricsData
         if (metrics.submissionTime >= beginSnapshot && metrics.completionTime <= endSnapshot)) {
      agg("numStages") += 1L
      agg("numTasks") += metrics.numTasks
      agg("stageDuration") += metrics.stageDuration
      agg("executorRunTime") += metrics.executorRunTime
      agg("executorCpuTime") += metrics.executorCpuTime
      agg("executorDeserializeTime") += metrics.executorDeserializeTime
      agg("executorDeserializeCpuTime") += metrics.executorDeserializeCpuTime
      agg("resultSerializationTime") += metrics.resultSerializationTime
      agg("jvmGCTime") += metrics.jvmGCTime
      agg("shuffleFetchWaitTime") += metrics.shuffleFetchWaitTime
      agg("shuffleWriteTime") += metrics.shuffleWriteTime
      agg("resultSize") = max(metrics.resultSize, agg("resultSize"))
      agg("diskBytesSpilled") += metrics.diskBytesSpilled
      agg("memoryBytesSpilled") += metrics.memoryBytesSpilled
      agg("peakExecutionMemory") += metrics.peakExecutionMemory
      agg("recordsRead") += metrics.recordsRead
      agg("bytesRead") += metrics.bytesRead
      agg("recordsWritten") += metrics.recordsWritten
      agg("bytesWritten") += metrics.bytesWritten
      agg("shuffleRecordsRead") += metrics.shuffleRecordsRead
      agg("shuffleTotalBlocksFetched") += metrics.shuffleTotalBlocksFetched
      agg("shuffleLocalBlocksFetched") += metrics.shuffleLocalBlocksFetched
      agg("shuffleRemoteBlocksFetched") += metrics.shuffleRemoteBlocksFetched
      agg("shuffleTotalBytesRead") += metrics.shuffleTotalBytesRead
      agg("shuffleLocalBytesRead") += metrics.shuffleLocalBytesRead
      agg("shuffleRemoteBytesRead") += metrics.shuffleRemoteBytesRead
      agg("shuffleRemoteBytesReadToDisk") += metrics.shuffleRemoteBytesReadToDisk
      agg("shuffleBytesWritten") += metrics.shuffleBytesWritten
      agg("shuffleRecordsWritten") += metrics.shuffleRecordsWritten
      submissionTime = min(metrics.submissionTime, submissionTime)
      completionTime = max(metrics.completionTime, completionTime)
    }
    agg("elapsedTime") = completionTime - submissionTime
    agg
  }

  // Extracts stages and their duration
  def stagesDuration() : LinkedHashMap[Int, Long] = {

    val stages : LinkedHashMap[Int, Long] = LinkedHashMap.empty[Int,Long]
    for (metrics <- listenerStage.stageMetricsData.sortBy(_.stageId)
         if (metrics.submissionTime >= beginSnapshot && metrics.completionTime <= endSnapshot)) {
      stages += (metrics.stageId -> metrics.stageDuration)
    }
    stages
  }

  // Custom aggregations and post-processing of metrics data
  def report(): String = {
    val aggregatedMetrics = aggregateStageMetrics()
    val stages = stagesDuration()
    var result = ListBuffer[String]()

    result = result :+ s"\nScheduling mode = ${sparkSession.sparkContext.getSchedulingMode.toString}"
    result = result :+ s"Spark Context default degree of parallelism = ${sparkSession.sparkContext.defaultParallelism}\n"
    result = result :+ "Aggregated Spark stage metrics:"

    aggregatedMetrics.foreach {
      case (metric: String, value: Long) =>
        result = result :+ Utils.prettyPrintValues(metric, value)
      case (_, _) => // We should no get here, in case add code to handle this
    }
    result.mkString("\n")

    // additional details on stages and their duration
    // can be switched off with a configuration
    if (stageInfoVerbose) {
      result = result :+ "\nStages and their duration:"
      stages.foreach {
        case (stageId: Int, duration: Long) =>
          result = result :+ Utils.prettyPrintValues(s"Stage $stageId duration", duration)
      }
    }

    result.mkString("\n")
  }

  // Custom aggregations and post-processing of executor metrics data with memory usage details
  // Note this report requires per-stage memory (executor metrics) data which is sent by the executors
  // at each heartbeat to the driver, there could be a small delay or the order of a few seconds
  // between the end of the job and the time the last metrics value is received
  // if you receive the error message java.util.NoSuchElementException: key not found:
  // retry to run the report after a few seconds
    def reportMemory(): String = {

    var result = ListBuffer[String]()
    val stages = {for (metrics <- listenerStage.stageMetricsData) yield metrics.stageId}.sorted

    // additional details on executor (memory) metrics
    result = result :+ "\nAdditional stage-level executor metrics (memory usage info):\n"

    stages.foreach {
      case (stageId: Int) =>
        for (metric <- executorMetricsNames) {
          val stageExecutorMetricsRaw = listenerStage.stageIdtoExecutorMetrics(stageId, metric)

          // find maximum metric value and corresponding executor
          val (executorMaxVal, maxVal) = stageExecutorMetricsRaw.maxBy(_._2)

          // This code is commented on purpose
          // It's there if in the future you want to have more complex metrics
          // 1. remove duplicate stageId values more precisely take the maximum value for each stageId
          // val stageExecutorMetrics  = stageExecutorMetricsRaw.groupBy(_._1).transform((_,v) => v.sortBy(_._2).last)
          // 2. find maximum value and corresponding executor value
          // val (executorMaxVal, maxVal) = stageExecutorMetrics.maxBy { case (key, value) => value }._2

          val messageHead = Utils.prettyPrintValues(s"Stage $stageId $metric maxVal bytes", maxVal)
          val messageTail =
            if (executorMaxVal != "driver") {
              s" on executor $executorMaxVal"
            } else {
            ""
            }
          result = result :+ (messageHead + messageTail)
        }
    }

    result.mkString("\n")
  }

  // Runs report and prints it
  def printReport(): Unit = {
    println(report())
  }

  // Runs the memory report and prints it
  def printMemoryReport(): Unit = {
    if (stageInfoVerbose) {
      println(reportMemory())
    }
    else {
      println("Collecting data for the memory is off")
      println("Check the value of spark.sparkmeasure.stageinfo.verbose")
    }
  }

  // Legacy transformation of data recorded from the custom Stage listener
  // into a DataFrame and register it as a view for querying with SQL
  def createStageMetricsDF(nameTempView: String = "PerfStageMetrics"): DataFrame = {
    import sparkSession.implicits._
    val resultDF = listenerStage.stageMetricsData.toSeq.toDF()
    resultDF.createOrReplaceTempView(nameTempView)
    logger.warn(s"Stage metrics data refreshed into temp view $nameTempView")
    resultDF
  }

  // Legacy metrics aggregation computed using SQL
  def aggregateStageMetrics(nameTempView: String = "PerfStageMetrics"): DataFrame = {
    sparkSession.sql(s"select count(*) as numStages, sum(numTasks) as numTasks, " +
      s"max(completionTime) - min(submissionTime) as elapsedTime, sum(stageDuration) as stageDuration , " +
      s"sum(executorRunTime) as executorRunTime, sum(executorCpuTime) as executorCpuTime, " +
      s"sum(executorDeserializeTime) as executorDeserializeTime, sum(executorDeserializeCpuTime) as executorDeserializeCpuTime, " +
      s"sum(resultSerializationTime) as resultSerializationTime, sum(jvmGCTime) as jvmGCTime, "+
      s"sum(shuffleFetchWaitTime) as shuffleFetchWaitTime, sum(shuffleWriteTime) as shuffleWriteTime, " +
      s"max(resultSize) as resultSize, " +
      s"sum(diskBytesSpilled) as diskBytesSpilled, sum(memoryBytesSpilled) as memoryBytesSpilled, " +
      s"max(peakExecutionMemory) as peakExecutionMemory, sum(recordsRead) as recordsRead, sum(bytesRead) as bytesRead, " +
      s"sum(recordsWritten) as recordsWritten, sum(bytesWritten) as bytesWritten, " +
      s"sum(shuffleRecordsRead) as shuffleRecordsRead, sum(shuffleTotalBlocksFetched) as shuffleTotalBlocksFetched, "+
      s"sum(shuffleLocalBlocksFetched) as shuffleLocalBlocksFetched, sum(shuffleRemoteBlocksFetched) as shuffleRemoteBlocksFetched, "+
      s"sum(shuffleTotalBytesRead) as shuffleTotalBytesRead, sum(shuffleLocalBytesRead) as shuffleLocalBytesRead, " +
      s"sum(shuffleRemoteBytesRead) as shuffleRemoteBytesRead, sum(shuffleRemoteBytesReadToDisk) as shuffleRemoteBytesReadToDisk, " +
      s"sum(shuffleBytesWritten) as shuffleBytesWritten, sum(shuffleRecordsWritten) as shuffleRecordsWritten " +
      s"from $nameTempView " +
      s"where submissionTime >= $beginSnapshot and completionTime <= $endSnapshot")

  }

  // Custom aggregations and post-processing of metrics data
  // This is legacy and uses Spark DataFrame operations,
  // use report instead, which will process data in the driver using Scala
  def reportUsingDataFrame(): String = {
    val nameTempView = "PerfStageMetrics"
    createStageMetricsDF(nameTempView)
    val aggregateDF = aggregateStageMetrics(nameTempView)
    var result = ListBuffer[String]()

    result = result :+ s"\nScheduling mode = ${sparkSession.sparkContext.getSchedulingMode.toString}"
    result = result :+ s"Spark Context default degree of parallelism = ${sparkSession.sparkContext.defaultParallelism}"

    /** Print a summary of the stage metrics. */
    val aggregateValues = aggregateDF.take(1)(0).toSeq
    if (aggregateValues(1) != null) {
      result = result :+ "Aggregated Spark stage metrics:"
      val cols = aggregateDF.columns
      result = result :+ (cols zip aggregateValues)
        .map{
          case(n:String, v:Long) => Utils.prettyPrintValues(n, v)
          case(n: String, null) => n + " => null"
          case(_,_) => ""
        }.mkString("\n")
    } else {
      result = result :+ " no data to report "
    }

    result.mkString("\n")
  }

  // Shortcut to run and measure the metrics for Spark execution, built after spark.time()
  def runAndMeasure[T](f: => T): T = {
    begin()
    val startTime = System.nanoTime()
    val ret = f
    val endTime = System.nanoTime()
    end()
    println(s"Time taken: ${(endTime - startTime) / 1000000} ms")
    printReport()
    ret
  }

  // Helper method to save data, we expect to have small amounts of data so collapsing to 1 partition seems OK
  def saveData(df: DataFrame, fileName: String, fileFormat: String = "json", saveMode: String = "default") = {
    df.coalesce(1).write.format(fileFormat).mode(saveMode).save(fileName)
    logger.warn(s"Stage metric data saved into $fileName using format=$fileFormat")
  }

  /**
   * Send the metrics to Prometheus.
   * serverIPnPort: String with prometheus pushgateway address, format is hostIP:Port,
   * metricsJob: job name,
   * labelName: metrics label name, default is sparkSession.sparkContext.appName,
   * labelValue: metrics label value, default is sparkSession.sparkContext.applicationId
   */
  def sendReportPrometheus(serverIPnPort: String,
                           metricsJob: String,
                           labelName: String = sparkSession.sparkContext.appName,
                           labelValue: String = sparkSession.sparkContext.applicationId): Unit = {

    val aggregatedMetrics = aggregateStageMetrics()

    /** Prepare a summary of the stage metrics for Prometheus. */
    val pushGateway = PushGateway(serverIPnPort, metricsJob)
    var str_metrics = s""

    aggregatedMetrics.foreach {
      case (metric: String, value: Long) =>
          str_metrics += pushGateway.validateMetric(metric.toLowerCase()) + s" " + value.toString + s"\n"
      }

    /** Send stage metrics to Prometheus. */
    val metricsType = s"stage"
    pushGateway.post(str_metrics, metricsType, labelName, labelValue)
  }

}
