package ch.cern.sparkmeasure

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.StandardCharsets
import java.util.Properties
import scala.util.Try

/**
 * KafkaSink: write Spark metrics and application info in near real-time to Kafka stream
 * use this mode to monitor Spark execution workload
 * use for Grafana dashboard and analytics of job execution
 *
 * How to use: attach the KafkaSink to a Spark Context using the extra listener infrastructure.
 * Example:
 * --conf spark.extraListeners=ch.cern.sparkmeasure.KafkaSink
 *
 * Configuration for KafkaSink is handled with Spark conf parameters:
 *
 * spark.sparkmeasure.kafkaBroker = Kafka broker endpoint URL
 * example: --conf spark.sparkmeasure.kafkaBroker=kafka.your-site.com:9092
 * spark.sparkmeasure.kafkaTopic = Kafka topic
 * example: --conf spark.sparkmeasure.kafkaTopic=sparkmeasure-stageinfo
 *
 * This code depends on "kafka clients", you may need to add the dependency:
 * --packages org.apache.kafka:kafka-clients:3.2.1
 *
 * Output: each message contains the name, it is acknowledged as metrics name as well.
 * Note: the amount of data generated is relatively small in most applications: O(number_of_stages)
 */
class KafkaSink(conf: SparkConf) extends SparkListener {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)
  logger.warn("Custom monitoring listener with Kafka sink initializing. Now attempting to connect to Kafka topic")

  // Initialize Kafka connection
  val (broker, topic) = Utils.parseKafkaConfig(conf, logger)
  private var producer: Producer[String, Array[Byte]] = _

  var appId: String = SparkSession.getActiveSession match {
    case Some(sparkSession) => sparkSession.sparkContext.applicationId
    case _ => "noAppId"
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    val executorInfo = executorAdded.executorInfo
    val epochMillis = System.currentTimeMillis()
    val metrics = Map[String, Any](
      "name" -> "executors_started",
      "appId" -> appId,
      "executorId" -> executorAdded.executorId,
      "host" -> executorInfo.executorHost,
      "totalCores" -> executorInfo.totalCores,
      "startTime" -> executorAdded.time,
      "epochMillis" -> epochMillis
    )
    report(metrics)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val submissionTime = stageSubmitted.stageInfo.submissionTime.getOrElse(0L)
    val attemptNumber = stageSubmitted.stageInfo.attemptNumber()
    val stageId = stageSubmitted.stageInfo.stageId.toString
    val epochMillis = System.currentTimeMillis()

    val metrics = Map[String, Any](
      "name" -> "stages_started",
      "appId" -> appId,
      "stageId" -> stageId,
      "attemptNumber" -> attemptNumber,
      "submissionTime" -> submissionTime,
      "epochMillis" -> epochMillis
    )
    report(metrics)
  }


  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageId = stageCompleted.stageInfo.stageId.toString
    val submissionTime = stageCompleted.stageInfo.submissionTime.getOrElse(0L)
    val completionTime = stageCompleted.stageInfo.completionTime.getOrElse(0L)
    val attemptNumber = stageCompleted.stageInfo.attemptNumber()
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
    report(stageEndMetrics)

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

    report(stageTaskMetrics)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    val epochMillis = System.currentTimeMillis()
    event match {
      case e: SparkListenerSQLExecutionStart =>
        val startTime = e.time
        val queryId = e.executionId.toString
        val description = e.description

        val queryStartMetrics = Map[String, Any](
          "name" -> "queries_started",
          "appId" -> appId,
          "description" -> description,
          "queryId" -> queryId,
          "startTime" -> startTime,
          "epochMillis" -> epochMillis
        )
        report(queryStartMetrics)
      case e: SparkListenerSQLExecutionEnd =>
        val endTime = e.time
        val queryId = e.executionId.toString

        val queryEndMetrics = Map[String, Any](
          "name" -> "queries_ended",
          "appId" -> appId,
          "queryId" -> queryId,
          "endTime" -> endTime,
          "epochMillis" -> epochMillis
        )
        report(queryEndMetrics)
      case _ => None // Ignore
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val startTime = jobStart.time
    val jobId = jobStart.jobId.toString
    val epochMillis = System.currentTimeMillis()

    val jobStartMetrics = Map[String, Any](
      "name" -> "jobs_started",
      "appId" -> appId,
      "jobId" -> jobId,
      "startTime" -> startTime,
      "epochMillis" -> epochMillis
    )
    report(jobStartMetrics)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val completionTime = jobEnd.time
    val jobId = jobEnd.jobId.toString
    val epochMillis = System.currentTimeMillis()

    val jobEndMetrics = Map[String, Any](
      "name" -> "jobs_ended",
      "appId" -> appId,
      "jobId" -> jobId,
      "completionTime" -> completionTime,
      "epochMillis" -> epochMillis
    )
    report(jobEndMetrics)
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    appId = applicationStart.appId.getOrElse("noAppId")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logger.info(s"Spark application ended, timestamp = ${applicationEnd.time}, closing Kafka connection.")
    synchronized(
      if (Option(producer).isDefined) {
        // Flush metrics & close the connection
        producer.flush()
        producer.close()

        // Reassign to null
        producer = null
      }
    )
  }


  protected def report[T <: Any](metrics: Map[String, T]): Unit = Try {
    ensureProducer()

    val str = IOUtils.writeToStringSerializedJSON(metrics)
    val message = str.getBytes(StandardCharsets.UTF_8)
    producer.send(new ProducerRecord[String, Array[Byte]](topic, message))
  }.recover {
    case ex: Throwable => logger.error(s"error on reporting metrics to kafka stream, details=${ex.getMessage}", ex)
  }

  private def ensureProducer(): Unit = {
    synchronized(
      if (Option(producer).isEmpty) {
        val props = new Properties()
        props.put("bootstrap.servers", broker)
        props.put("retries", "10")
        props.put("batch.size", "16384")
        props.put("linger.ms", "0")
        props.put("buffer.memory", "16384000")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", classOf[ByteArraySerializer].getName)
        props.put("client.id", "spark-measure")
        producer = new KafkaProducer(props)
      }
    )
  }

}

/**
 * KafkaSinkExtended extends the basic KafkaSink functionality with a verbose dump of tasks metrics
 * Note: this can generate a large amount of data O(Number_of_tasks)
 * Configuration parameters and how-to use: see KafkaSink
 */
class KafkaSinkExtended(conf: SparkConf) extends KafkaSink(conf) {

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

    val point2 = Map[String, Any](
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
      "epochMillis" -> epochMillis
    )
    report(point2)
  }
}