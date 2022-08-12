package ch.cern.sparkmeasure

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable.{LinkedHashMap, ListBuffer}
import scala.math.max

/**
  *  Task Metrics: collects metrics data at Task granularity
  *                and provides aggregation and reporting functions for the end-user
  *
  * Example of how to use task metrics:
  * val taskMetrics = ch.cern.sparkmeasure.TaskMetrics(spark)
  * taskMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show)
  *
  * The tool is based on using Spark Listeners as data source and collecting metrics in a ListBuffer of
  * a case class that encapsulates Spark task metrics.
  *
  */
case class TaskMetrics(sparkSession: SparkSession) {

  lazy val logger = LoggerFactory.getLogger(this.getClass.getName)

  // This starts inserts and starts the custom Spark Listener into the live Spark Context
  val listenerTask = new TaskInfoRecorderListener()
  registerListener(sparkSession, listenerTask)

  // Variables used to store the start and end time of the period of interest for the metrics report
  var beginSnapshot: Long = 0L
  var endSnapshot: Long = 0L

  // Marks the beginning of data collection
  def begin(): Long = {
    listenerTask.taskMetricsData.clear()    // clear previous data to reduce memory footprint
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
  def registerListener(spark: SparkSession, listener: TaskInfoRecorderListener): Unit = {
    spark.sparkContext.addSparkListener(listener)
  }

  // helper method to remove the listener
  def removeListener(): Unit = {
    sparkSession.sparkContext.removeSparkListener(listenerTask)
  }

  // Compute basic aggregation on the Task metrics for the metrics report
  // also filter on the time boundaries for the report
  def aggregateTaskMetrics() : LinkedHashMap[String, Long] = {

    val agg = Utils.zeroMetricsTask()

    for (metrics <- listenerTask.taskMetricsData
         if (metrics.launchTime >= beginSnapshot && metrics.finishTime <= endSnapshot)) {
      agg("numTasks") += 1L
      if (metrics.successful) {
        agg("successful tasks") += 1L
      }
      if (metrics.speculative) {
        agg("speculative tasks") += 1L
      }
      agg("taskDuration") += metrics.duration
      agg("schedulerDelayTime") += metrics.schedulerDelay
      agg("executorRunTime") += metrics.executorRunTime
      agg("executorCpuTime") += metrics.executorCpuTime
      agg("executorDeserializeTime") += metrics.executorDeserializeTime
      agg("executorDeserializeCpuTime") += metrics.executorDeserializeCpuTime
      agg("resultSerializationTime") += metrics.resultSerializationTime
      agg("jvmGCTime") += metrics.jvmGCTime
      agg("shuffleFetchWaitTime") += metrics.shuffleFetchWaitTime
      agg("shuffleWriteTime") += metrics.shuffleWriteTime
      agg("gettingResultTime") += metrics.gettingResultTime
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
    }
    agg
  }

  // Custom aggregations and post-processing of metrics data
  def report(): String = {
    val aggregatedMetrics = aggregateTaskMetrics()
    var result = ListBuffer[String]()

    result = result :+ (s"\nScheduling mode = ${sparkSession.sparkContext.getSchedulingMode.toString}")
    result = result :+ (s"Spark Context default degree of parallelism = ${sparkSession.sparkContext.defaultParallelism}\n")
    result = result :+ ("Aggregated Spark task metrics:")

    aggregatedMetrics.foreach {
      case (metric: String, value: Long) =>
        result = result :+ Utils.prettyPrintValues(metric, value)
    }
    result.mkString("\n")
  }

  def printReport(): Unit = {
    println(report())
  }

  // Legacy transformation of data recorded from the custom Stage listener
  // into a DataFrame and register it as a view for querying with SQL
  def createTaskMetricsDF(nameTempView: String = "PerfTaskMetrics"): DataFrame = {
    import sparkSession.implicits._
    val resultDF = listenerTask.taskMetricsData.toSeq.toDF()
    resultDF.createOrReplaceTempView(nameTempView)
    logger.warn(s"Stage metrics data refreshed into temp view $nameTempView")
    resultDF
  }

  // legacy metrics aggregation computed using SQL
  def aggregateTaskMetrics(nameTempView: String = "PerfTaskMetrics"): DataFrame = {
    sparkSession.sql(s"select count(*) as numtasks, " +
      s"max(finishTime) - min(launchTime) as elapsedTime, sum(duration) as duration, sum(schedulerDelay) as schedulerDelayTime, " +
      s"sum(executorRunTime) as executorRunTime, sum(executorCpuTime) as executorCpuTime, " +
      s"sum(executorDeserializeTime) as executorDeserializeTime, sum(executorDeserializeCpuTime) as executorDeserializeCpuTime, " +
      s"sum(resultSerializationTime) as resultSerializationTime, sum(jvmGCTime) as jvmGCTime, "+
      s"sum(shuffleFetchWaitTime) as shuffleFetchWaitTime, sum(shuffleWriteTime) as shuffleWriteTime, " +
      s"sum(gettingResultTime) as gettingResultTime, " +
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
      s"where launchTime >= $beginSnapshot and finishTime <= $endSnapshot")
  }

  // Custom aggregations and post-processing of metrics data
  // This is legacy and uses Spark DataFrame operations,
  // use report instead, which will process data in the driver using Scala
  def reportUsingDataFrame(): String = {
    var result = ListBuffer[String]()
    val nameTempView = "PerfTaskMetrics"
    createTaskMetricsDF(nameTempView)
    val aggregateDF = aggregateTaskMetrics(nameTempView)

    result = result :+ (s"\nScheduling mode = ${sparkSession.sparkContext.getSchedulingMode.toString}")
    result = result :+ (s"Spark Context default degree of parallelism = ${sparkSession.sparkContext.defaultParallelism}")
    result = result :+ ("Aggregated Spark task metrics:")

    /** Print a summary of the task metrics. */
    val aggregateValues = aggregateDF.take(1)(0).toSeq
    val cols = aggregateDF.columns
    result = result :+ (cols zip aggregateValues)
      .map {
        case(n: String, v: Long) => Utils.prettyPrintValues(n, v)
        case(n: String, null) => n + " => null"
        case(_,_) => ""
      }.mkString("\n")

    result.mkString("\n")
  }

  // Shortcut to run and measure the metrics for Spark execution, built after spark.time()
  def runAndMeasure[T](f: => T): T = {
    this.begin()
    val startTime = System.nanoTime()
    val ret = f
    val endTime = System.nanoTime()
    this.end()
    println(s"Time taken: ${(endTime - startTime) / 1000000} ms")
    printReport()
    ret
  }

  // helper method to save data, we expect to have moderate amounts of data so collapsing to 1 partition seems OK
  def saveData(df: DataFrame, fileName: String, fileFormat: String = "json", saveMode: String = "default") = {
    df.repartition(1).write.format(fileFormat).mode(saveMode).save(fileName)
    logger.warn(s"Task metric data saved into $fileName using format=$fileFormat")
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

    val aggregatedMetrics = aggregateTaskMetrics()

    /** Prepare a summary of the task metrics for Prometheus. */
    val pushGateway = PushGateway(serverIPnPort, metricsJob)
    var str_metrics = s""

    aggregatedMetrics.foreach {
      case (metric: String, value: Long) =>
          str_metrics += pushGateway.validateMetric(metric.toLowerCase()) + s" " + value.toString + s"\n"
      }

    /** Send task metrics to Prometheus. */
    val metricsType = s"task"
    pushGateway.post(str_metrics, metricsType, labelName, labelValue)
  }

}
