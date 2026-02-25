package ch.cern.sparkmeasure

import org.apache.spark.scheduler._
import org.apache.spark.{SparkConf, TaskFailedReason, TaskKilled}

import scala.collection.mutable
import scala.util.Try

/**
 * AppEventKafkaSink: An extended version of ch.cern.sparkmeasure.KafkaSink that adds
 * application-level metrics and custom fields support.
 *
 * This listener combines:
 * 1. All stage/executor/query metrics from the base KafkaSink
 * 2. Application-level metrics (executor counts, stage counts, task counts)
 * 3. Custom fields passed via spark configurations
 * 4. Enhanced application_started and applications_ended events with metadata
 *
 * Configuration:
 * --conf spark.extraListeners=com.example.listener.AppEventKafkaSink
 * --conf spark.sparkmeasure.kafkaBroker=kafka.example.com:9092
 * --conf spark.sparkmeasure.kafkaTopic=spark-metrics
 * --conf spark.sparkmeasure.kafka.* = Additional Kafka properties
 * --conf spark.sparkmeasure.customFields.* = Custom metadata to include in app events
 *
 * More configuration parameters and how-to use: see KafkaSink
 *
 * Example custom configs:
 * --conf spark.sparkmeasure.customFields.project=my-project
 * --conf spark.sparkmeasure.customFields.environment=production
 * --conf spark.sparkmeasure.customFields.team=engineering
 *
 */
class AppEventKafkaSink(conf: SparkConf) extends KafkaSink(conf) {

  // Application tracking
  private var appName: String = "unknown"
  private var startTime: Long = 0L

  // Executor tracking
  private val executorIds: mutable.HashSet[String] = mutable.HashSet.empty[String]
  private var totalExecutorCount: Int = 0
  private var executorsFailed: Int = 0
  private var executorsKilled: Int = 0

  // Job tracking
  private var totalJobsCompleted: Int = 0
  private var succeededJobsCount: Int = 0
  private var failedJobsCount: Int = 0

  // Stage tracking
  private var totalStagesCompleted: Int = 0
  private var succeededStagesCount: Int = 0
  private var failedStagesCount: Int = 0

  // Task tracking
  private var totalTaskCount: Int = 0
  private var numTaskFailed: Int = 0
  private var numTaskKilled: Int = 0

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    super.onApplicationStart(applicationStart)

    appId = applicationStart.appId.getOrElse("noAppId")
    appName = applicationStart.appName
    startTime = applicationStart.time
    val customFields = extractCustomFields(conf)

    val epochMillis = System.currentTimeMillis()

    val appStartMetrics = Map[String, Any](
      "name" -> "application_started",
      "appId" -> appId,
      "appName" -> appName,
      "startTime" -> startTime,
      "epochMillis" -> epochMillis
    ) ++ customFields

    report(appStartMetrics)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    super.onStageCompleted(stageCompleted)

    if (stageCompleted != null && stageCompleted.stageInfo != null) {
      val stageInfo = stageCompleted.stageInfo
      totalStagesCompleted += 1

      if (stageInfo.failureReason.isDefined) {
        failedStagesCount += 1
      } else {
        succeededStagesCount += 1
      }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    if (taskEnd != null) {
      totalTaskCount += 1

      if (taskEnd.reason != null) {
        taskEnd.reason match {
          case _: TaskKilled =>
            numTaskKilled += 1
          case _: TaskFailedReason =>
            numTaskFailed += 1
          case _ =>
        }
      }
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    super.onJobEnd(jobEnd)

    if (jobEnd != null) {
      totalJobsCompleted += 1

      jobEnd.jobResult match {
        case org.apache.spark.scheduler.JobSucceeded =>
          succeededJobsCount += 1
        case _ =>
          failedJobsCount += 1
      }
    }
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    super.onExecutorAdded(executorAdded)

    if (executorAdded != null && executorAdded.executorId != null) {
      val execId = executorAdded.executorId
      if (!executorIds.contains(execId) && execId != "driver") {
        executorIds += execId
        totalExecutorCount += 1
      }
    }
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    super.onExecutorRemoved(executorRemoved)

    if (executorRemoved != null && executorRemoved.reason != null) {
      executorRemoved.reason match {
        case reason if reason.toLowerCase.contains("kill") =>
          executorsKilled += 1
        case _ =>
          executorsFailed += 1
      }
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    val completionTime = applicationEnd.time

    val safeEndTime = if (completionTime > 0) completionTime else System.currentTimeMillis()
    val duration = if (startTime > 0) safeEndTime - startTime else 0L
    val successful = succeededJobsCount > 0 && failedJobsCount == 0
    val epochMillis = System.currentTimeMillis()
    val configurations = conf.getAll.toMap
    val customFields = extractCustomFields(conf)

    val appEndMetrics = Map[String, Any](
      "name" -> "applications_ended",
      "appId" -> appId,
      "appName" -> appName,
      "startTime" -> startTime,
      "completionTime" -> completionTime,
      "duration" -> duration,
      "successful" -> successful,
      "totalExecutorCount" -> totalExecutorCount,
      "executorsFailed" -> executorsFailed,
      "executorsKilled" -> executorsKilled,
      "totalJobsCompleted" -> totalJobsCompleted,
      "succeededJobsCount" -> succeededJobsCount,
      "failedJobsCount" -> failedJobsCount,
      "numStagesCompleted" -> totalStagesCompleted,
      "numSucceededStages" -> succeededStagesCount,
      "numFailedStages" -> failedStagesCount,
      "totalTaskCount" -> totalTaskCount,
      "numTaskFailed" -> numTaskFailed,
      "numTaskKilled" -> numTaskKilled,
      "epochMillis" -> epochMillis,
      "configurations" -> configurations
    ) ++ customFields

    report(appEndMetrics)
    super.onApplicationEnd(applicationEnd)
  }

  private def extractCustomFields(conf: SparkConf): Map[String, String] = {
    Try {
      conf.getAll
        .filter { case (key, _) => key.startsWith("spark.sparkmeasure.customFields.") }
        .map { case (key, value) => (key.stripPrefix("spark.sparkmeasure.customFields."), value) }
        .toMap
    }.getOrElse(Map.empty[String, String])
  }
}

/**
 * AppEventKafkaSinkExtended extends the AppEventKafkaSink functionality with a verbose dump of tasks metrics
 * Note: this can generate a large amount of data O(Number_of_tasks)
 * Configuration parameters and how-to use: see AppEventKafkaSinkExtended
 */
class AppEventKafkaSinkExtended(conf: SparkConf) extends AppEventKafkaSink(conf) {

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val taskInfo = taskStart.taskInfo
    val epochMillis = System.currentTimeMillis()

    val taskStartMetrics = Map[String, Any](
      "name" -> "tasks_started",
      "appId" -> appId,
      "taskId" -> taskInfo.taskId,
      "attemptNumber" -> taskInfo.attemptNumber,
      "stageId" -> taskStart.stageId,
      "launchTime" -> taskInfo.launchTime,
      "epochMillis" -> epochMillis
    )
    report(taskStartMetrics)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    super.onTaskEnd(taskEnd)

    val taskInfo = taskEnd.taskInfo
    val taskmetrics = taskEnd.taskMetrics
    val epochMillis = System.currentTimeMillis()

    val point1 = Map[String, Any](
      "name" -> "tasks_ended",
      "appId" -> appId,
      "taskId" -> taskInfo.taskId,
      "attemptNumber" -> taskInfo.attemptNumber,
      "stageId" -> taskEnd.stageId,
      "launchTime" -> taskInfo.launchTime,
      "finishTime" -> taskInfo.finishTime,
      "epochMillis" -> epochMillis
    )
    report(point1)

    val taskMetricsHeader = Map[String, Any](
      "name" -> "task_metrics",
      "appId" -> appId,
      // task info
      "taskId" -> taskInfo.taskId,
      "attemptNumber" -> taskInfo.attemptNumber,
      "stageId" -> taskEnd.stageId,
      "launchTime" -> taskInfo.launchTime,
      "finishTime" -> taskInfo.finishTime,
      "failed" -> taskInfo.failed,
      "speculative" -> taskInfo.speculative,
      "killed" -> taskInfo.killed,
      "finished" -> taskInfo.finished,
      "executorId" -> taskInfo.executorId,
      "duration" -> taskInfo.duration,
      "successful" -> taskInfo.successful,
      "host" -> taskInfo.host,
      "taskLocality" -> Utils.encodeTaskLocality(taskInfo.taskLocality),
      "epochMillis" -> epochMillis
    )
    val taskMetricsBody = if (taskmetrics != null) Map[String, Any](
      // task metrics
      "executorRunTime" -> taskmetrics.executorRunTime,
      "executorCpuTime" -> taskmetrics.executorCpuTime,
      "executorDeserializeCpuTime" -> taskmetrics.executorDeserializeCpuTime,
      "executorDeserializeTime" -> taskmetrics.executorDeserializeTime,
      "jvmGCTime" -> taskmetrics.jvmGCTime,
      "memoryBytesSpilled" -> taskmetrics.memoryBytesSpilled,
      "peakExecutionMemory" -> taskmetrics.peakExecutionMemory,
      "resultSerializationTime" -> taskmetrics.resultSerializationTime,
      "resultSize" -> taskmetrics.resultSize,
      "bytesRead" -> taskmetrics.inputMetrics.bytesRead,
      "recordsRead" -> taskmetrics.inputMetrics.recordsRead,
      "bytesWritten" -> taskmetrics.outputMetrics.bytesWritten,
      "recordsWritten" -> taskmetrics.outputMetrics.recordsWritten,
      "shuffleTotalBytesRead" -> taskmetrics.shuffleReadMetrics.totalBytesRead,
      "shuffleRemoteBytesRead" -> taskmetrics.shuffleReadMetrics.remoteBytesRead,
      "shuffleLocalBytesRead" -> taskmetrics.shuffleReadMetrics.localBytesRead,
      "shuffleTotalBlocksFetched" -> taskmetrics.shuffleReadMetrics.totalBlocksFetched,
      "shuffleLocalBlocksFetched" -> taskmetrics.shuffleReadMetrics.localBlocksFetched,
      "shuffleRemoteBlocksFetched" -> taskmetrics.shuffleReadMetrics.remoteBlocksFetched,
      "shuffleRecordsRead" -> taskmetrics.shuffleReadMetrics.recordsRead,
      // this requires spark2.3 and above "remoteBytesReadToDisk" -> taskmetrics.shuffleReadMetrics.remoteBytesReadToDisk,
      "shuffleFetchWaitTime" -> taskmetrics.shuffleReadMetrics.fetchWaitTime,
      "shuffleBytesWritten" -> taskmetrics.shuffleWriteMetrics.bytesWritten,
      "shuffleRecordsWritten" -> taskmetrics.shuffleWriteMetrics.recordsWritten,
      "shuffleWriteTime" -> taskmetrics.shuffleWriteMetrics.writeTime,
    ) else Map()
    val point2 = taskMetricsHeader ++ taskMetricsBody
    report(point2)
  }
}