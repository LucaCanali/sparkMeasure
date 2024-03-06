package ch.cern.sparkmeasure

import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

/**
 * PushGatewaySink: write Spark metrics and application info in near real-time to Prometheus Push Gateway
 * use this mode to monitor Spark execution workload
 * use for Grafana dashboard and analytics of job execution
 * Limitation: only metrics with numeric values are reported to the Push Gateway
 *
 * How to use: attach the PushGatewaySink to a Spark Context using the extra listener infrastructure.
 * Example:
 * --conf spark.extraListeners=ch.cern.sparkmeasure.PushGatewaySink
 *
 * Configuration for PushGatewaySink is handled with Spark conf parameters:
 * spark.sparkmeasure.pushgateway = SERVER:PORT // Prometheus Push Gateway URL
 * spark.sparkmeasure.pushgateway.jobname // value for the job label, default pushgateway
 * Example: --conf spark.sparkmeasure.pushgateway=localhost:9091
 *
 * Output: each message contains the metric name and value, only numeric values are used
 * Note: the amount of data generated is relatively small in most applications: O(number_of_stages)
 */
class PushGatewaySink(conf: SparkConf) extends SparkListener {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)
  logger.warn("Custom monitoring listener with Prometheus Push Gateway sink initializing. Now attempting to connect to the Push Gateway")

  // Initialize PushGateway connection
  val (url, job) = Utils.parsePushGatewayConfig(conf, logger)
  val gateway = PushGateway(url, job)

  var appId: String = SparkSession.getActiveSession match {
    case Some(sparkSession) => sparkSession.sparkContext.applicationId
    case _ => "noAppId"
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val submissionTime = stageSubmitted.stageInfo.submissionTime.getOrElse(0L)
    val attemptNumber = stageSubmitted.stageInfo.attemptNumber().toLong
    val stageId = stageSubmitted.stageInfo.stageId.toLong
    val epochMillis = System.currentTimeMillis()

    val metrics = Map[String, Any](
      "name" -> "stages_started",
      "appId" -> appId,
      "stageId" -> stageId,
      "attemptNumber" -> attemptNumber,
      "submissionTime" -> submissionTime,
      "epochMillis" -> epochMillis
    )
    report(s"stageSubmitted-${stageId}-${attemptNumber}", metrics)
  }


  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageId = stageCompleted.stageInfo.stageId.toLong
    val submissionTime = stageCompleted.stageInfo.submissionTime.getOrElse(0L)
    val completionTime = stageCompleted.stageInfo.completionTime.getOrElse(0L)
    val attemptNumber = stageCompleted.stageInfo.attemptNumber().toLong
    val epochMillis = System.currentTimeMillis()

    // Report overall metrics
    val stageEndMetrics = Map[String, Any](
      "name" -> "stages_ended",
      "appId" -> appId,
      "stageId" -> stageId,
      "attemptNumber" -> attemptNumber,
      "submissionTime" -> submissionTime,
      "completionTime" -> completionTime,
      "epochMillis" -> epochMillis
    )
    report(s"stageEnd-${stageId}-${attemptNumber}", stageEndMetrics)

    // Report stage task metric
    val taskMetrics = stageCompleted.stageInfo.taskMetrics
    val stageTaskMetrics = Map[String, Any](
      "name" -> "stage_metrics",
      "appId" -> appId,
      "stageId" -> stageId,
      "attemptNumber" -> attemptNumber,
      "submissionTime" -> submissionTime,
      "completionTime" -> completionTime,
      "failureReason" -> stageCompleted.stageInfo.failureReason.getOrElse(""),
      "executorRunTime" -> taskMetrics.executorRunTime,
      "executorCpuTime" -> taskMetrics.executorRunTime,
      "executorDeserializeCpuTime" -> taskMetrics.executorDeserializeCpuTime,
      "executorDeserializeTime" -> taskMetrics.executorDeserializeTime,
      "jvmGCTime" -> taskMetrics.jvmGCTime,
      "memoryBytesSpilled" -> taskMetrics.memoryBytesSpilled,
      "peakExecutionMemory" -> taskMetrics.peakExecutionMemory,
      "resultSerializationTime" -> taskMetrics.resultSerializationTime,
      "resultSize" -> taskMetrics.resultSize,
      "bytesRead" -> taskMetrics.inputMetrics.bytesRead,
      "recordsRead" -> taskMetrics.inputMetrics.recordsRead,
      "bytesWritten" -> taskMetrics.outputMetrics.bytesWritten,
      "recordsWritten" -> taskMetrics.outputMetrics.recordsWritten,
      "shuffleTotalBytesRead" -> taskMetrics.shuffleReadMetrics.totalBytesRead,
      "shuffleRemoteBytesRead" -> taskMetrics.shuffleReadMetrics.remoteBytesRead,
      "shuffleRemoteBytesReadToDisk" -> taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk,
      "shuffleLocalBytesRead" -> taskMetrics.shuffleReadMetrics.localBytesRead,
      "shuffleTotalBlocksFetched" -> taskMetrics.shuffleReadMetrics.totalBlocksFetched,
      "shuffleLocalBlocksFetched" -> taskMetrics.shuffleReadMetrics.localBlocksFetched,
      "shuffleRemoteBlocksFetched" -> taskMetrics.shuffleReadMetrics.remoteBlocksFetched,
      "shuffleRecordsRead" -> taskMetrics.shuffleReadMetrics.recordsRead,
      "shuffleFetchWaitTime" -> taskMetrics.shuffleReadMetrics.fetchWaitTime,
      "shuffleBytesWritten" -> taskMetrics.shuffleWriteMetrics.bytesWritten,
      "shuffleRecordsWritten" -> taskMetrics.shuffleWriteMetrics.recordsWritten,
      "shuffleWriteTime" -> taskMetrics.shuffleWriteMetrics.writeTime,
      "epochMillis" -> epochMillis
    )

    report(s"stageMetrics-${stageId}-${attemptNumber}", stageTaskMetrics)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    val epochMillis = System.currentTimeMillis()
    event match {
      case e: SparkListenerSQLExecutionStart =>
        val startTime = e.time
        val queryId = e.executionId
        val description = e.description

        val queryStartMetrics = Map[String, Any](
          "name" -> "queries_started",
          "appId" -> appId,
          "description" -> description,
          "queryId" -> queryId,
          "startTime" -> startTime,
          "epochMillis" -> epochMillis
        )
        report(s"queryStart-${queryId}", queryStartMetrics)
      case e: SparkListenerSQLExecutionEnd =>
        val endTime = e.time
        val queryId = e.executionId

        val queryEndMetrics = Map[String, Any](
          "name" -> "queries_ended",
          "appId" -> appId,
          "queryId" -> queryId,
          "endTime" -> endTime,
          "epochMillis" -> epochMillis
        )
        report(s"queryEnd-${queryId}", queryEndMetrics)
      case _ => None // Ignore
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val startTime = jobStart.time
    val jobId = jobStart.jobId.toLong
    val epochMillis = System.currentTimeMillis()

    val jobStartMetrics = Map[String, Any](
      "name" -> "jobs_started",
      "appId" -> appId,
      "jobId" -> jobId,
      "startTime" -> startTime,
      "epochMillis" -> epochMillis
    )
    report(s"jobStart-${jobId}", jobStartMetrics)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val completionTime = jobEnd.time
    val jobId = jobEnd.jobId.toLong
    val epochMillis = System.currentTimeMillis()

    val jobEndMetrics = Map[String, Any](
      "name" -> "jobs_ended",
      "appId" -> appId,
      "jobId" -> jobId,
      "completionTime" -> completionTime,
      "epochMillis" -> epochMillis
    )
    report(s"jobEnd-${jobId}", jobEndMetrics)
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    appId = applicationStart.appId.getOrElse("noAppId")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logger.info(s"Spark application ended, timestamp = ${applicationEnd.time}")
  }


  protected def report[T <: Any](metricsType: String, metrics: Map[String, T]): Unit = Try {

    var strMetrics = s""
    metrics.foreach {
      case (metric: String, value: Long) =>
        strMetrics += gateway.validateMetric(metric.toLowerCase()) + s" " + value.toString + s"\n"
      case (_, _) => // Discard
    }

    gateway.post(strMetrics, metricsType, "appid", appId)

  }.recover {
    case ex: Throwable => logger.error(s"error on reporting metrics to Push Gateway, details=${ex.getMessage}", ex)
  }

}
