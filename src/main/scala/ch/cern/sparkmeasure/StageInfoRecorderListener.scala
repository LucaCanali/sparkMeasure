package ch.cern.sparkmeasure

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.scheduler._

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer


// contains the list of task metrics and other measurements of interest at the Stage level,
// packaged into a case class
case class StageVals (jobId: Int, jobGroup:String, stageId: Int, name: String,
                 submissionTime: Long, completionTime: Long, stageDuration: Long, numTasks: Int,
                 executorRunTime: Long, executorCpuTime: Long,
                 executorDeserializeTime: Long, executorDeserializeCpuTime: Long,
                 resultSerializationTime: Long, jvmGCTime: Long, resultSize: Long,
                 diskBytesSpilled: Long, memoryBytesSpilled: Long, peakExecutionMemory: Long, recordsRead: Long,
                 bytesRead: Long, recordsWritten: Long, bytesWritten: Long,
                 shuffleFetchWaitTime: Long, shuffleTotalBytesRead: Long, shuffleTotalBlocksFetched: Long,
                 shuffleLocalBlocksFetched: Long, shuffleRemoteBlocksFetched: Long, shuffleLocalBytesRead: Long,
                 shuffleRemoteBytesRead: Long, shuffleRemoteBytesReadToDisk: Long, shuffleRecordsRead: Long,
                 shuffleWriteTime: Long, shuffleBytesWritten: Long, shuffleRecordsWritten: Long
                )

/**
 * StageInfoRecorderListener: this listener gathers metrics with Stage execution granularity
 * It is based on the Spark Listener interface
 * Stage metrics are stored in memory and use to produce a report that aggregates resource consumption
 * they can also be consumed "raw" (transformed into a DataFrame and/or saved to a file)
 * See StageMetrics
 */
class StageInfoRecorderListener(stageInfoVerbose: Boolean,
                                executorMetricNames: Array[String]) extends SparkListener {

  val stageMetricsData: ListBuffer[StageVals] = ListBuffer.empty[StageVals]
  val stageIdtoJobId: HashMap[Int, Int] = HashMap.empty[Int, Int]
  val stageIdtoJobGroup: HashMap[Int, String] = HashMap.empty[Int, String]
  val stageIdtoExecutorMetrics: HashMap[(Int, String), ListBuffer[(String, Long)]] =
    HashMap.empty[(Int, String), ListBuffer[(String, Long)]]

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobStart.stageIds.foreach(stageId => stageIdtoJobId += (stageId -> jobStart.jobId))
    val group = jobStart.properties.getProperty("spark.jobGroup.id")
    if (group != null) {
      jobStart.stageIds.foreach(stageId => stageIdtoJobGroup += (stageId -> group))
    }
  }

  /**
   * This methods fires at the end of the stage and collects metrics flattened into the stageMetricsData ListBuffer
   * Note all reported times are in ms, cpu time and shuffle write time are originally in nanoseconds,
   * thus in the code are divided by 1 million to normalize them to milliseconds
   */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    val taskMetrics = stageInfo.taskMetrics
    val jobId = stageIdtoJobId(stageInfo.stageId)
    val group = if (stageIdtoJobGroup.contains(stageInfo.stageId)) {
          stageIdtoJobGroup(stageInfo.stageId)
        }
        else { null }
    val currentStage = StageVals(jobId, group, stageInfo.stageId, stageInfo.name,
      stageInfo.submissionTime.getOrElse(0L), stageInfo.completionTime.getOrElse(0L),
      stageInfo.completionTime.getOrElse(0L) - stageInfo.submissionTime.getOrElse(0L),
      stageInfo.numTasks, taskMetrics.executorRunTime, taskMetrics.executorCpuTime / 1000000,
      taskMetrics.executorDeserializeTime, taskMetrics.executorDeserializeCpuTime / 1000000,
      taskMetrics.resultSerializationTime, taskMetrics.jvmGCTime, taskMetrics.resultSize,
      taskMetrics.diskBytesSpilled, taskMetrics.memoryBytesSpilled,
      taskMetrics.peakExecutionMemory,
      taskMetrics.inputMetrics.recordsRead, taskMetrics.inputMetrics.bytesRead,
      taskMetrics.outputMetrics.recordsWritten, taskMetrics.outputMetrics.bytesWritten,
      taskMetrics.shuffleReadMetrics.fetchWaitTime, taskMetrics.shuffleReadMetrics.totalBytesRead,
      taskMetrics.shuffleReadMetrics.totalBlocksFetched, taskMetrics.shuffleReadMetrics.localBlocksFetched,
      taskMetrics.shuffleReadMetrics.remoteBlocksFetched,
      taskMetrics.shuffleReadMetrics.localBytesRead, taskMetrics.shuffleReadMetrics.remoteBytesRead,
      taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk, taskMetrics.shuffleReadMetrics.recordsRead,
      taskMetrics.shuffleWriteMetrics.writeTime / 1000000, taskMetrics.shuffleWriteMetrics.bytesWritten,
      taskMetrics.shuffleWriteMetrics.recordsWritten
    )
    stageMetricsData += currentStage

  }

  // Record executor metrics detailed per stage, this provides peak memory utilization values
  // This uses a feature introduced in Spark 3.1.0
  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    if (stageInfoVerbose) {
      val execId = executorMetricsUpdate.execId
      val executorUpdates = executorMetricsUpdate.executorUpdates

        executorUpdates.foreach {
          case ((stageId: Int, attemptNum: Int), executorMetrics: ExecutorMetrics) =>
            // driver metrics are associated with stageId=-1, we filter those out
            if (stageId >= 0) {
            // Loop for each executor metric name we want to capture, which is configurable
            // executor metrics doc, see https://spark.apache.org/docs/latest/monitoring.html#executor-metrics
            for (metric <- executorMetricNames) {
              // Add Map Key if it does not exist, the arrival of new stages will require adding new entries
              if (stageIdtoExecutorMetrics.getOrElse((stageId, metric), ListBuffer()).isEmpty) {
                stageIdtoExecutorMetrics += ((stageId, metric) -> ListBuffer())
              }
              // Add  the latest measurement to the List of metrics values
              // There could be multiple entries for each executor in the ListBuffer
              // We will deal with when building the report
              stageIdtoExecutorMetrics((stageId, metric)) +=
                ((execId, executorMetrics.getMetricValue(metric)))
            }
        }
      }
    }
  }

}
