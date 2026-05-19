package ch.cern.sparkmeasure

import org.apache.spark.scheduler._
import org.apache.spark.{SparkConf, TaskFailedReason, TaskKilled}

import scala.collection.mutable
import scala.util.Try


/**
 * An extended version of ch.cern.sparkmeasure.KafkaSink that adds
 * application-level metrics and custom labels support.
 *
 * This listener combines:
 * 1. All stage/executor/query metrics from the base KafkaSink
 * 2. Application-level metrics (executor counts, stage counts, task counts)
 * 3. Custom Labels passed via spark configurations
 * 4. Enhanced applications_started and applications_ended events with metadata
 *
 * Configuration:
 * --conf spark.extraListeners=ch.cern.sparkmeasure.KafkaSinkV2
 * --conf spark.sparkmeasure.kafkaBroker=kafka.example.com:9092
 * --conf spark.sparkmeasure.kafkaTopic=spark-metrics
 * --conf spark.sparkmeasure.kafka.* = Additional Kafka properties
 * --conf spark.sparkmeasure.appLabels.* = Custom metadata to include in app events
 *
 * Example custom configs:
 * --conf spark.sparkmeasure.appLabels.project=my-project
 * --conf spark.sparkmeasure.appLabels.environment=production
 * --conf spark.sparkmeasure.appLabels.team=engineering
 *
 */
class KafkaSinkV2(conf: SparkConf) extends KafkaSink(conf) {

  // Application tracking
  private var appName: String = "unknown"
  private var startTime: Long = 0L

  // Executor tracking
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
    val appLabels = extractAppLabels(conf)

    val epochMillis = System.currentTimeMillis()

    val appStartMetrics = Map[String, Any](
      "name" -> "applications_started",
      "appId" -> appId,
      "appName" -> appName,
      "startTime" -> startTime,
      "epochMillis" -> epochMillis
    ) ++ appLabels

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

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {

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
    val epochMillis = System.currentTimeMillis()
    val sparkConfigs = extractSparkConfigs(conf)
    val appLabels = extractAppLabels(conf)

    val appEndMetrics = Map[String, Any](
      "name" -> "applications_ended",
      "appId" -> appId,
      "appName" -> appName,
      "startTime" -> startTime,
      "completionTime" -> completionTime,
      "duration" -> duration,
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
      "sparkConfigs" -> sparkConfigs
    ) ++ appLabels

    report(appEndMetrics)
    super.onApplicationEnd(applicationEnd)
  }

  private def extractAppLabels(conf: SparkConf): Map[String, String] = {
    Try {
      conf.getAll
        .filter { case (key, _) => key.startsWith("spark.sparkmeasure.appLabels.") }
        .map { case (key, value) => (key.stripPrefix("spark.sparkmeasure."), value) }
        .toMap
    }.getOrElse(Map.empty[String, String])
  }

  private def extractSparkConfigs(conf: SparkConf): Map[String, String] = {
    val optimizationConfigKeys = List(
      // Executor resource configurations
      "spark.executor.instances",
      "spark.executor.memory",
      "spark.executor.memoryOverhead",
      "spark.executor.cores",
      "spark.executor.heartbeatInterval",

      // Driver resource configurations
      "spark.driver.memory",
      "spark.driver.memoryOverhead",
      "spark.driver.cores",
      "spark.driver.maxResultSize",

      // Dynamic allocation
      "spark.dynamicAllocation.enabled",
      "spark.dynamicAllocation.minExecutors",
      "spark.dynamicAllocation.maxExecutors",
      "spark.dynamicAllocation.initialExecutors",
      "spark.dynamicAllocation.executorIdleTimeout",
      "spark.dynamicAllocation.cachedExecutorIdleTimeout",
      "spark.dynamicAllocation.schedulerBacklogTimeout",

      // Shuffle and partitioning
      "spark.sql.shuffle.partitions",
      "spark.default.parallelism",
      "spark.sql.adaptive.enabled",
      "spark.sql.adaptive.coalescePartitions.enabled",
      "spark.sql.adaptive.coalescePartitions.minPartitionNum",
      "spark.sql.adaptive.advisoryPartitionSizeInBytes",
      "spark.sql.adaptive.skewJoin.enabled",
      "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
      "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",

      // Shuffle service and compression
      "spark.shuffle.service.enabled",
      "spark.shuffle.compress",
      "spark.shuffle.spill.compress",
      "spark.shuffle.file.buffer",
      "spark.shuffle.io.retryWait",
      "spark.shuffle.io.maxRetries",

      // Memory management
      "spark.memory.fraction",
      "spark.memory.storageFraction",
      "spark.memory.offHeap.enabled",
      "spark.memory.offHeap.size",

      // Kubernetes resource configurations
      "spark.kubernetes.executor.request.cores",
      "spark.kubernetes.executor.limit.cores",
      "spark.kubernetes.driver.request.cores",
      "spark.kubernetes.driver.limit.cores",
      "spark.kubernetes.memoryOverheadFactor",
      "spark.kubernetes.allocation.batch.size",
      "spark.kubernetes.allocation.batch.delay",
      "spark.kubernetes.executor.deleteOnTermination",

      // Serialization
      "spark.serializer",
      "spark.kryoserializer.buffer.max",
      "spark.kryoserializer.buffer",
      "spark.rdd.compress",

      // Network and timeouts
      "spark.network.timeout",
      "spark.rpc.askTimeout",
      "spark.rpc.lookupTimeout",

      // SQL optimization
      "spark.sql.autoBroadcastJoinThreshold",
      "spark.sql.broadcastTimeout",
      "spark.sql.files.maxPartitionBytes",
      "spark.sql.files.openCostInBytes",
      "spark.sql.files.minPartitionNum",

      // Speculation
      "spark.speculation",
      "spark.speculation.multiplier",
      "spark.speculation.quantile",
      "spark.speculation.interval"
    )
    Try {
      conf.getAll.toMap.filter { case (key, _) =>
        optimizationConfigKeys.contains(key)
      }
    }.getOrElse(Map.empty[String, String])
  }
}

/**
 * KafkaSinkV2Extended extends the basic KafkaSinkV2 functionality with a verbose dump of tasks metrics
 * Note: this can generate a large amount of data O(Number_of_tasks)
 * Configuration parameters and how-to use: see KafkaSinkV2Extended
 */
class KafkaSinkV2Extended(conf: SparkConf) extends KafkaSinkV2(conf) {

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