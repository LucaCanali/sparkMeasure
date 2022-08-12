package ch.cern.sparkmeasure

import org.apache.spark.scheduler._

import scala.collection.mutable.{HashMap, ListBuffer}

// Contains the list of task metrics and other measurements of interest at the Task level, as a case class
case class TaskVals(jobId: Int, jobGroup: String, stageId: Int, index: Long, launchTime: Long, finishTime: Long,
                    duration: Long, schedulerDelay: Long, executorId: String, host: String, taskLocality: Int,
                    speculative: Boolean, gettingResultTime: Long, successful: Boolean,
                    executorRunTime: Long, executorCpuTime: Long,
                    executorDeserializeTime: Long, executorDeserializeCpuTime: Long,
                    resultSerializationTime: Long, jvmGCTime: Long, resultSize: Long,
                    diskBytesSpilled: Long, memoryBytesSpilled: Long, peakExecutionMemory: Long, recordsRead: Long,
                    bytesRead: Long, recordsWritten: Long, bytesWritten: Long,
                    shuffleFetchWaitTime: Long, shuffleTotalBytesRead: Long, shuffleTotalBlocksFetched: Long,
                    shuffleLocalBlocksFetched: Long, shuffleRemoteBlocksFetched: Long, shuffleLocalBytesRead: Long,
                    shuffleRemoteBytesRead: Long, shuffleRemoteBytesReadToDisk: Long, shuffleRecordsRead: Long,
                    shuffleWriteTime: Long, shuffleBytesWritten: Long, shuffleRecordsWritten: Long)


/**
 * TaskInfoRecorderListener: this listener gathers metrics with Task execution granularity
 * It is based on the Spark Listener interface
 * Task metrics are stored in memory and use to produce a report that aggregates resource consumption
 * they can also be consumed "raw" (transformed into a DataFrame and/or saved to a file)
 */
class TaskInfoRecorderListener() extends SparkListener {

  val taskMetricsData: ListBuffer[TaskVals] = ListBuffer.empty[TaskVals]
  val StageIdtoJobId: HashMap[Int, Int] = HashMap.empty[Int, Int]
  val StageIdtoJobGroup: HashMap[Int, String] = HashMap.empty[Int, String]

  // Collects information made available at job start
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobStart.stageIds.foreach(stageId => StageIdtoJobId += (stageId -> jobStart.jobId))
    val group = jobStart.properties.getProperty("spark.jobGroup.id")
    if (group != null) {
      jobStart.stageIds.foreach(stageId => StageIdtoJobGroup += (stageId -> group))
    }
  }

  /**
    * This methods fires at the end of each Task and collects metrics flattened into the taskMetricsData ListBuffer
    * Note all times are in ms, cpu time and shufflewrite are originally in nanosec, thus in the code are divided by 1e6
    */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskInfo = taskEnd.taskInfo
    val taskMetrics = taskEnd.taskMetrics
    val gettingResultTime = {
      if (taskInfo.gettingResultTime == 0L) 0L
      else taskInfo.finishTime - taskInfo.gettingResultTime
    }
    val duration = taskInfo.finishTime - taskInfo.launchTime
    val jobId = StageIdtoJobId(taskEnd.stageId)
    val group = if (StageIdtoJobGroup.contains(taskEnd.stageId)) {
      StageIdtoJobGroup(taskEnd.stageId)
    }
    else { null }
    val currentTask = TaskVals(jobId, group, taskEnd.stageId, taskInfo.taskId, taskInfo.launchTime,
                               taskInfo.finishTime, duration,
                               math.max(0L, duration - taskMetrics.executorRunTime - taskMetrics.executorDeserializeTime -
        taskMetrics.resultSerializationTime - gettingResultTime),
                               taskInfo.executorId, taskInfo.host, Utils.encodeTaskLocality(taskInfo.taskLocality),
                               taskInfo.speculative, gettingResultTime, taskInfo.successful,
                               taskMetrics.executorRunTime, taskMetrics.executorCpuTime / 1000000,
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
                               taskMetrics.shuffleWriteMetrics.recordsWritten)
    taskMetricsData += currentTask

  }
}
