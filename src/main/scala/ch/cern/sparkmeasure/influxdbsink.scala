package ch.cern.sparkmeasure

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerEvent, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskStart, TaskLocality}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.influxdb.InfluxDBFactory
import org.influxdb.BatchOptions
import org.influxdb.dto.Point
import java.util.concurrent.TimeUnit

import org.slf4j.LoggerFactory

/**
 * InfluxDBSink: write Spark metrics and application info in near real-time to InfluxDB
 *  use this mode to monitor Spark execution workload
 *  use for Grafana dashboard and analytics of job execution
 *  How to use: attach the InfluxDBSInk to a Spark Context using the extra listener infrastructure.
 *  Example:
 *  --conf spark.extraListeners=ch.cern.sparkmeasure.InfluxDBSink
 *
 *  Configuration for InfluxDBSink is handled with Spark conf parameters:
 *
 *  spark.sparkmeasure.influxdbURL, example value: http://mytestInfluxDB:8086
 *  spark.sparkmeasure.influxdbUsername (can be empty)
 *  spark.sparkmeasure.influxdbPassword (can be empty)
 *  spark.sparkmeasure.influxdbName, defaults to "sparkmeasure"
 *  spark.sparkmeasure.influxdbStagemetrics, boolean, default is false
 *
 * This code depends on "influxdb.java", you may need to add the dependency:
 *  --packages org.influxdb:influxdb-java:2.14
 *
 * InfluxDBExtended: provides additional and verbose info on Task execution
 *  use: --conf spark.extraListeners=ch.cern.sparkmeasure.InfluxDBSinkExtended
 *
 * InfluxDBSink: the amount of data generated is relatively small in most applications: O(number_of_stages)
 * InfluxDBSInkExtended can generate a large amount of data O(Number_of_tasks), use with care
 */
class InfluxDBSink(conf: SparkConf) extends SparkListener {

  lazy val logger = LoggerFactory.getLogger(this.getClass.getName)
  logger.warn(s"Custom monitoring listener with InfluxDB sink initializing, now attempting to connect to InfluxDB")

  // Initialize InfluxDB connection
  val url = Utils.parseInfluxDBURL(conf, logger)
  val (username, password) = Utils.parseInfluxDBCredentials(conf, logger)

  // Tries to connect to InfluxDB, using the given URL and credentials
  val influxDB =  username match {
    case username if username.isEmpty =>
      // no username and password, InfluxDB must be running with auth-enabled=false
      InfluxDBFactory.connect(url)
    case _ => InfluxDBFactory.connect(url, username, password)
  }

  val dbName = Utils.parseInfluxDBName(conf, logger)
  if (!influxDB.databaseExists(dbName)) {
    influxDB.createDatabase(dbName)
  }
  val database = influxDB.setDatabase(dbName)
  logger.info((s"using INfluxDB database $dbName"))

  val logStageMetrics = Utils.parseInfluxDBStagemetrics(conf, logger)

  // Flush every 1000 Points, at least every 1000ms
  influxDB.enableBatch(BatchOptions.DEFAULTS.actions(1000).flushDuration(1000))

  var appId = "noAppId"

  appId = SparkSession.getActiveSession match {
    case Some(sparkSession) => sparkSession.sparkContext.applicationId
    case _ => "noAppId"
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val submissionTime = stageSubmitted.stageInfo.submissionTime.getOrElse(0L)
    val attemptNumber = stageSubmitted.stageInfo.attemptNumber()
    val stageId = stageSubmitted.stageInfo.stageId
    val point = Point.measurement("stages_started")
      .tag("applicationId", appId)
      .addField("stageId", stageId)
      .addField("attemptNUmber", attemptNumber)
      .time(submissionTime, TimeUnit.MILLISECONDS)
      .build()
    database.write(point)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageId = stageCompleted.stageInfo.stageId
    val submissionTime = stageCompleted.stageInfo.submissionTime.getOrElse(0L)
    val completionTime = stageCompleted.stageInfo.completionTime.getOrElse(0L)
    val attemptNumber = stageCompleted.stageInfo.attemptNumber()

    val point1 = Point.measurement("stages_ended")
      .tag("applicationId", appId)
      .time(completionTime, TimeUnit.MILLISECONDS)
      .addField("stageId", stageId)
      .addField("attemptNumber", attemptNumber)
      .addField("submissionTime", submissionTime)
      .build()
    database.write(point1)

    if (logStageMetrics) {
      val taskmetrics = stageCompleted.stageInfo.taskMetrics
      // TODO: add all the available metrics
      val point2 = Point.measurement("stage_metrics")
        .tag("applicationId", appId)
        .time(completionTime, TimeUnit.MILLISECONDS)
        .addField("stageId", stageId)
        .addField("attemptNumber", attemptNumber)
        .addField("failureReason", stageCompleted.stageInfo.failureReason.getOrElse(""))
        .addField("submissionTime", submissionTime)
        .addField("completionTime", completionTime)
        .addField("executorRunTime", taskmetrics.executorRunTime)
        .addField("executorCpuTime", taskmetrics.executorCpuTime)
        .addField("executorDeserializeCpuTime", taskmetrics.executorDeserializeCpuTime)
        .addField("executorDeserializeTime", taskmetrics.executorDeserializeTime)
        .addField("jvmGCTime", taskmetrics.jvmGCTime)
        .addField("memoryBytesSpilled", taskmetrics.memoryBytesSpilled)
        .addField("peakExecutionMemory", taskmetrics.peakExecutionMemory)
        .addField("resultSerializationTime", taskmetrics.resultSerializationTime)
        .addField("resultSize", taskmetrics.resultSize)
        .addField("bytesRead", taskmetrics.inputMetrics.bytesRead)
        .addField("recordsRead", taskmetrics.inputMetrics.recordsRead)
        .addField("bytesWritten", taskmetrics.outputMetrics.bytesWritten)
        .addField("recordsWritten", taskmetrics.outputMetrics.recordsWritten)
        .addField("shuffleTotalBytesRead", taskmetrics.shuffleReadMetrics.totalBytesRead)
        .addField("shuffleRemoteBytesRead", taskmetrics.shuffleReadMetrics.remoteBytesRead)
        .addField("shuffleLocalBytesRead", taskmetrics.shuffleReadMetrics.localBytesRead)
        .addField("shuffleTotalBlocksFetched", taskmetrics.shuffleReadMetrics.totalBlocksFetched)
        .addField("shuffleLocalBlocksFetched", taskmetrics.shuffleReadMetrics.localBlocksFetched)
        .addField("shuffleRemoteBlocksFetched", taskmetrics.shuffleReadMetrics.remoteBlocksFetched)
        .addField("shuffleRecordsRead", taskmetrics.shuffleReadMetrics.recordsRead)
        // this requires spark2.3 and above .addField("remoteBytesReadToDisk", taskmetrics.shuffleReadMetrics.remoteBytesReadToDisk)
        .addField("shuffleFetchWaitTime", taskmetrics.shuffleReadMetrics.fetchWaitTime)
        .addField("shuffleBytesWritten", taskmetrics.shuffleWriteMetrics.bytesWritten)
        .addField("shuffleRecordsWritten", taskmetrics.shuffleWriteMetrics.recordsWritten)
        .addField("shuffleWriteTime", taskmetrics.shuffleWriteMetrics.writeTime)
        .build()
      database.write(point2)
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerSQLExecutionStart => {
        val startTime = e.time
        val queryId = e.executionId
        val description = e.description
        // val details = e.details

        val point = Point.measurement("queries_started")
          .tag("applicationId", appId)
          .time(startTime, TimeUnit.MILLISECONDS)
          .addField("description", description)
          .addField("queryId", queryId)
          .build()
        database.write(point)
      }
      case e: SparkListenerSQLExecutionEnd => {
        val endTime = e.time
        val queryId = e.executionId

        val point = Point.measurement("queries_ended")
          .tag("applicationId", appId)
          .time(endTime, TimeUnit.MILLISECONDS)
          .addField("queryId", queryId)
          .build()
        database.write(point)
      }
      case _ => None // Ignore
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val startTime = jobStart.time
    val jobId = jobStart.jobId

    val point = Point.measurement("jobs_started")
      .tag("applicationId", appId)
      .time(startTime, TimeUnit.MILLISECONDS)
      .addField("jobID", jobId)
      .build()
    database.write(point)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val completionTime = jobEnd.time
    val jobId = jobEnd.jobId

    val point = Point.measurement("jobs_ended")
      .tag("applicationId", appId)
      .time(completionTime, TimeUnit.MILLISECONDS)
      .addField("jobID", jobId)
      .build()
    database.write(point)
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    appId = applicationStart.appId.getOrElse("noAppId")
    // val appName = applicationStart.appName
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logger.info(s"Spark application ended, timestamp = ${applicationEnd.time}, closing InfluxDB connection.")
    influxDB.close()
  }

}

/**
 * InfluxDBSinkExtended extends the basic Influx Sink functionality with a verbose dump of
 * Task metrics and task info into InfluxDB
 * Note: this can generate a large amount of data O(Number_of_tasks)
 * Configuration parameters and how-to use: see InfluxDBSink
 */
class InfluxDBSinkExtended(conf: SparkConf) extends InfluxDBSink(conf: SparkConf) {

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val taskInfo = taskStart.taskInfo
    val point = Point.measurement("tasks_started")
      .tag("applicationId", appId)
      .time(taskInfo.launchTime, TimeUnit.MICROSECONDS)
      .addField("taskId", taskInfo.taskId)
      .addField("attemptNumber", taskInfo.attemptNumber)
      .addField("stageId", taskStart.stageId)
      .build()
    database.write(point)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskInfo = taskEnd.taskInfo
    val taskmetrics = taskEnd.taskMetrics

    val point1 = Point.measurement("tasks_ended")
      .tag("applicationId", appId)
      .time(taskInfo.finishTime, TimeUnit.MILLISECONDS)
      .addField("taskId", taskInfo.taskId)
      .addField("attemptNumber", taskInfo.attemptNumber)
      .addField("launchTime", taskInfo.launchTime)
      .addField("stageId", taskEnd.stageId)
      .build()
    database.write(point1)

    val point2 = Point.measurement("task_metrics")
      .tag("applicationId", appId)
      .time(taskInfo.finishTime, TimeUnit.MILLISECONDS)
      // task info
      .addField("taskId", taskInfo.taskId)
      .addField("attemptNumber", taskInfo.attemptNumber)
      .addField("stageId", taskEnd.stageId)
      .addField("launchTime", taskInfo.launchTime)
      .addField("completionTime", taskInfo.finishTime)
      .addField("failed", taskInfo.failed)
      .addField("speculative", taskInfo.speculative)
      .addField("killed", taskInfo.killed)
      .addField("finished", taskInfo.finished)
      .addField("executorId", taskInfo.executorId)
      .addField("duration", taskInfo.duration)
      .addField("successful", taskInfo.successful)
      .addField("host", taskInfo.host)
      .addField("taskLocality", Utils.encodeTaskLocality(taskInfo.taskLocality))
      // task metrics
      .addField("executorRunTime", taskmetrics.executorRunTime)
      .addField("executorCpuTime", taskmetrics.executorCpuTime)
      .addField("executorDeserializeCpuTime", taskmetrics.executorDeserializeCpuTime)
      .addField("executorDeserializeTime", taskmetrics.executorDeserializeTime)
      .addField("jvmGCTime", taskmetrics.jvmGCTime)
      .addField("memoryBytesSpilled", taskmetrics.memoryBytesSpilled)
      .addField("peakExecutionMemory", taskmetrics.peakExecutionMemory)
      .addField("resultSerializationTime", taskmetrics.resultSerializationTime)
      .addField("resultSize", taskmetrics.resultSize)
      .addField("bytesRead", taskmetrics.inputMetrics.bytesRead)
      .addField("recordsRead", taskmetrics.inputMetrics.recordsRead)
      .addField("bytesWritten", taskmetrics.outputMetrics.bytesWritten)
      .addField("recordsWritten", taskmetrics.outputMetrics.recordsWritten)
      .addField("shuffleTotalBytesRead", taskmetrics.shuffleReadMetrics.totalBytesRead)
      .addField("shuffleRemoteBytesRead", taskmetrics.shuffleReadMetrics.remoteBytesRead)
      .addField("shuffleLocalBytesRead", taskmetrics.shuffleReadMetrics.localBytesRead)
      .addField("shuffleTotalBlocksFetched", taskmetrics.shuffleReadMetrics.totalBlocksFetched)
      .addField("shuffleLocalBlocksFetched", taskmetrics.shuffleReadMetrics.localBlocksFetched)
      .addField("shuffleRemoteBlocksFetched", taskmetrics.shuffleReadMetrics.remoteBlocksFetched)
      .addField("shuffleRecordsRead", taskmetrics.shuffleReadMetrics.recordsRead)
      // this requires spark2.3 and above .addField("remoteBytesReadToDisk", taskmetrics.shuffleReadMetrics.remoteBytesReadToDisk)
      .addField("shuffleFetchWaitTime", taskmetrics.shuffleReadMetrics.fetchWaitTime)
      .addField("shuffleBytesWritten", taskmetrics.shuffleWriteMetrics.bytesWritten)
      .addField("shuffleRecordsWritten", taskmetrics.shuffleWriteMetrics.recordsWritten)
      .addField("shuffleWriteTime", taskmetrics.shuffleWriteMetrics.writeTime)
      .build()
    database.write(point2)
  }
}
