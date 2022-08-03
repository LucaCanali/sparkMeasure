package ch.cern.sparkmeasure

import collection.mutable.HashMap

import org.apache.spark.scheduler._

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

// Accumulators contain task metrics and other metrics, such as SQL metrics, this case class is used to process them
case class StageAccumulablesInfo (jobId: Int, stageId: Int, submissionTime: Long, completionTime: Long,
                                  accId: Long, name: String, value: Long)

/**
 * StageInfoRecorderListener: this listener gathers metrics with Stage execution granularity
 * It is based on the Spark Listener interface
 * Stage metrics are stored in memory and can be consumed "raw" (to a file, to Kafka, InfluxDB, Prometheus)
 * or used in a form of report that aggregates resource consumption
 * See StageMetrics
 */
class StageInfoRecorderListener extends SparkListener {

  val stageMetricsData: ListBuffer[StageVals] = ListBuffer.empty[StageVals]
  val accumulablesMetricsData: ListBuffer[StageAccumulablesInfo] = ListBuffer.empty[StageAccumulablesInfo]
  val StageIdtoJobId: HashMap[Int, Int] = HashMap.empty[Int, Int]
  val StageIdtoJobGroup: HashMap[Int, String] = HashMap.empty[Int, String]

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobStart.stageIds.foreach(stageId => StageIdtoJobId += (stageId -> jobStart.jobId))
    val group = jobStart.properties.getProperty("spark.jobGroup.id")
    if (group != null) {
      jobStart.stageIds.foreach(stageId => StageIdtoJobGroup += (stageId -> group))
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
    val jobId = StageIdtoJobId(stageInfo.stageId)
    val group = if (StageIdtoJobGroup.contains(stageInfo.stageId)) {
          StageIdtoJobGroup(stageInfo.stageId)
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
}
