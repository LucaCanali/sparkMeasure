package ch.cern.sparkmeasure

import org.apache.log4j.LogManager
import org.apache.spark.scheduler._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
 * Spark Measure package: proof-of-concept tool for measuring Spark performance metrics
 *   This is based on using Spark Listeners as data source and collecting metrics in a ListBuffer
 *   The list buffer is then transformed into a DataFrame for analysis
 *
 *  Stage Metrics: collects and aggregates metrics at the end of each stage
 *  Task Metrics: collects data at task granularity
 *
 * Use modes:
 *   Interactive mode from the REPL
 *   Flight recorder mode: records data and saves it for later processing
 *
 * Supported languages:
 *   The tool is written in Scala, but it can be used both from Scala and Python
 *
 * Example usage for stage metrics:
 * val stageMetrics = new ch.cern.sparkmeasure.StageMetrics(spark)
 * stageMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show)
 *
 * for task metrics:
 * val taskMetrics = new ch.cern.sparkmeasure.TaskMetrics(spark)
 * spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
 * val df = taskMetrics.createTaskMetricsDF()
 *
 * To use in flight recorder mode add:
 * --conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics
 *
 * Created by Luca.Canali@cern.ch, March 2017
 * Tested on Spark 2.1
 *
 */

case class TaskVals(jobId: Int, stageId: Int, index: Long, launchTime: Long, finishTime: Long,
                duration: Long, schedulerDelay: Long, executorId: String, host: String, taskLocality: Int,
                speculative: Boolean, gettingResultTime: Long, successful: Boolean,
                executorRunTime: Long, executorCpuTime: Long,
                executorDeserializeTime: Long, executorDeserializeCpuTime: Long,
                resultSerializationTime: Long, jvmGCTime: Long, resultSize: Long, numUpdatedBlockStatuses: Int,
                diskBytesSpilled: Long, memoryBytesSpilled: Long, peakExecutionMemory: Long, recordsRead: Long,
                bytesRead: Long, recordsWritten: Long, bytesWritten: Long,
                shuffleFetchWaitTime: Long, shuffleTotalBytesRead: Long, shuffleTotalBlocksFetched: Long,
                shuffleLocalBlocksFetched: Long, shuffleRemoteBlocksFetched: Long, shuffleWriteTime: Long,
                shuffleBytesWritten: Long, shuffleRecordsWritten: Long)

class TaskInfoRecorderListener extends SparkListener {

  var currentJobId: Int = 0
  val taskMetricsData: ListBuffer[TaskVals] = ListBuffer.empty[TaskVals]

  def encodeTaskLocality(taskLocality: TaskLocality.TaskLocality): Int = {
    taskLocality match {
      case TaskLocality.PROCESS_LOCAL => 0
      case TaskLocality.NODE_LOCAL => 1
      case TaskLocality.RACK_LOCAL => 2
      case TaskLocality.NO_PREF => 3
      case TaskLocality.ANY => 4
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    currentJobId = jobStart.jobId
  }

  /**
    * This methods fires at the end of the Task and collects metrics flattened into the taskMetricsData ListBuffer
    * Note all times are in ms, cpu time and shufflewrite are originally in nanosec, thus in the code are divided by 1e6
    */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskInfo = taskEnd.taskInfo
    val taskMetrics = taskEnd.taskMetrics
    val currentTask = TaskVals(currentJobId, taskEnd.stageId, taskInfo.taskId, taskInfo.launchTime,
      taskInfo.finishTime, taskInfo.finishTime - taskInfo.launchTime,
      math.max(0L, taskInfo.finishTime - taskInfo.launchTime -
        taskMetrics.executorRunTime - taskMetrics.executorDeserializeTime - taskMetrics.resultSerializationTime),
      taskInfo.executorId, taskInfo.host, encodeTaskLocality(taskInfo.taskLocality),
      taskInfo.speculative, taskInfo.gettingResultTime, taskInfo.successful,
      taskMetrics.executorRunTime, taskMetrics.executorCpuTime / 1000000,
      taskMetrics.executorDeserializeTime, taskMetrics.executorDeserializeCpuTime / 1000000,
      taskMetrics.resultSerializationTime, taskMetrics.jvmGCTime, taskMetrics.resultSize,
      taskMetrics.updatedBlockStatuses.length, taskMetrics.diskBytesSpilled, taskMetrics.memoryBytesSpilled,
      taskMetrics.peakExecutionMemory,
      taskMetrics.inputMetrics.recordsRead, taskMetrics.inputMetrics.bytesRead,
      taskMetrics.outputMetrics.recordsWritten, taskMetrics.outputMetrics.bytesWritten,
      taskMetrics.shuffleReadMetrics.fetchWaitTime, taskMetrics.shuffleReadMetrics.totalBytesRead,
      taskMetrics.shuffleReadMetrics.totalBlocksFetched, taskMetrics.shuffleReadMetrics.localBlocksFetched,
      taskMetrics.shuffleReadMetrics.remoteBlocksFetched,
      taskMetrics.shuffleWriteMetrics.writeTime / 1000000, taskMetrics.shuffleWriteMetrics.bytesWritten,
      taskMetrics.shuffleWriteMetrics.recordsWritten
    )
    taskMetricsData += currentTask
  }
}

case class TaskMetrics(sparkSession: SparkSession) {

  lazy val logger = LogManager.getLogger("TaskMetrics")

  /** This inserts the custom Spark Listener into the live Spark Context */
  val listenerTask = new TaskInfoRecorderListener
  sparkSession.sparkContext.addSparkListener(listenerTask)

  def createTaskMetricsDF(nameTempView: String = "PerfTaskMetrics"): DataFrame = {
    import sparkSession.implicits._
    val resultDF = listenerTask.taskMetricsData.toDF
    resultDF.createOrReplaceTempView(nameTempView)
    logger.warn(s"Stage metrics data refreshed into temp view $nameTempView")
    resultDF
  }

  /** helper method to save data, we expect to have moderate amounts of data so collapsing to 1 partition seems OK */
  def saveData(df: DataFrame, fileName: String, fileFormat: String = "json") = {
    df.orderBy("jobId", "stageId", "index").repartition(1).write.format(fileFormat).save(fileName)
    logger.warn(s"Task metric data saved into $fileName using format=$fileFormat")
  }

}
