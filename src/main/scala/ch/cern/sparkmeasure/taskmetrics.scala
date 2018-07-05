package ch.cern.sparkmeasure

import org.apache.spark.scheduler._
import org.apache.spark.sql.{DataFrame, SparkSession }
import org.slf4j.LoggerFactory
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
  * val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)
  * stageMetrics.runAndMeasure(spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show)
  *
  * for task metrics:
  * val taskMetrics = ch.cern.sparkmeasure.TaskMetrics(spark)
  * spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
  * val df = taskMetrics.createTaskMetricsDF()
  *
  * To use in flight recorder mode add:
  * --conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics
  *
  * Created by Luca.Canali@cern.ch, March 2017
  *
  */

case class TaskVals(jobId: Int, jobGroup: String, stageId: Int, index: Long, launchTime: Long, finishTime: Long,
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

case class TaskAccumulablesInfo(jobId: Int, stageId: Int, taskId: Long, submissionTime: Long, finishTime: Long,
                                accId: Long, name: String, value: Long)

class TaskInfoRecorderListener(gatherAccumulables: Boolean = false) extends SparkListener {

  val taskMetricsData: ListBuffer[TaskVals] = ListBuffer.empty[TaskVals]
  val accumulablesMetricsData: ListBuffer[TaskAccumulablesInfo] = ListBuffer.empty[TaskAccumulablesInfo]
  val StageIdtoJobId: collection.mutable.HashMap[Int, Int] = collection.mutable.HashMap.empty[Int, Int]
  val StageIdtoJobGroup: collection.mutable.HashMap[Int, String] = collection.mutable.HashMap.empty[Int, String]

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
    jobStart.stageIds.foreach(stageId => StageIdtoJobId += (stageId -> jobStart.jobId))
    val group = jobStart.properties.getProperty("spark.jobGroup.id")
    if (group != null) {
      jobStart.stageIds.foreach(stageId => StageIdtoJobGroup += (stageId -> group))
    }
  }

  /**
    * This methods fires at the end of the Task and collects metrics flattened into the taskMetricsData ListBuffer
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
                               taskInfo.executorId, taskInfo.host, encodeTaskLocality(taskInfo.taskLocality),
                               taskInfo.speculative, gettingResultTime, taskInfo.successful,
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
                               taskMetrics.shuffleWriteMetrics.recordsWritten)
    taskMetricsData += currentTask

    /**
      * Collect data from accumulators (includes task metrics and SQL metrics)
      * as this can be a lot of data, only gather data if gatherAccumulables is true
      * note the additional filters to keep only numerical values to make this code simpler
      * note and todo: gatherAccumulables for TaskMetrics implementation currently works only for spark 2.1.x,
      * this feature is broken on 2.2.1 as a consequence of [SPARK PR 17596](https://github.com/apache/spark/pull/17596)
      */
    if (gatherAccumulables) {
      taskInfo.accumulables.foreach(acc => try {
        val value = acc.value.getOrElse(0L).asInstanceOf[Long]
        val name = acc.name.getOrElse("")
        val currentAccumulablesInfo = TaskAccumulablesInfo(jobId, taskEnd.stageId, taskInfo.taskId,
                                                           taskInfo.launchTime, taskInfo.finishTime, acc.id, name, value)
        accumulablesMetricsData += currentAccumulablesInfo
      }
      catch {
        case ex: ClassCastException => None
      })
    }

  }
}

case class TaskMetrics(sparkSession: SparkSession, gatherAccumulables: Boolean = false) {

  lazy val logger = LoggerFactory.getLogger(getClass)

  /** This inserts the custom Spark Listener into the live Spark Context */
  val listenerTask = new TaskInfoRecorderListener(gatherAccumulables)
  sparkSession.sparkContext.addSparkListener(listenerTask)

  /** Variables used to store the start and end time of the period of interest for the metrics report */
  var beginSnapshot: Long = 0L
  var endSnapshot: Long = 0L

  def begin(): Long = {
    listenerTask.taskMetricsData.clear()    // clear previous data to reduce memory footprint
    beginSnapshot = System.currentTimeMillis()
    beginSnapshot
  }

  def end(): Long = {
    endSnapshot = System.currentTimeMillis()
    endSnapshot
  }

  def createAccumulablesDF(nameTempView: String = "AccumulablesTaskMetrics"): DataFrame = {
    import sparkSession.implicits._
    val resultDF = listenerTask.accumulablesMetricsData.toDF
    resultDF.createOrReplaceTempView(nameTempView)
    logger.warn(s"Accumulables metrics data refreshed into temp view $nameTempView")
    resultDF
  }

  def printAccumulables(): Unit = {
    val accumulableTaskMetrics = "AccumulablesTaskMetrics"
    val aggregatedAccumulables = "AggregatedAccumulables"
    createAccumulablesDF(accumulableTaskMetrics)

    sparkSession.sql(s"select accId, name, max(value) as endValue " +
      s"from $accumulableTaskMetrics " +
      s"where submissionTime >= $beginSnapshot and finishTime <= $endSnapshot " +
      s"group by accId, name").createOrReplaceTempView(aggregatedAccumulables)

    val internalMetricsDf = sparkSession.sql(s"select name, sum(endValue) " +
      s"from $aggregatedAccumulables " +
      s"where name like 'internal.metrics%' " +
      s"group by name")
    println("\nAggregated Spark accumulables of type internal.metric:")
    internalMetricsDf.show(200, false)

    val otherAccumulablesDf = sparkSession.sql(s"select accId, name, endValue " +
      s"from $aggregatedAccumulables " +
      s"where name not like 'internal.metrics%' ")
    println("\nSpark accumulables by accId of type != internal.metric:")
    otherAccumulablesDf.show(200, false)
  }

  def createTaskMetricsDF(nameTempView: String = "PerfTaskMetrics"): DataFrame = {
    import sparkSession.implicits._
    val resultDF = listenerTask.taskMetricsData.toDF
    resultDF.createOrReplaceTempView(nameTempView)
    logger.warn(s"Stage metrics data refreshed into temp view $nameTempView")
    resultDF
  }

  def report(): String = {
    var result = ListBuffer[String]()

    createTaskMetricsDF("PerfTaskMetrics")
    val aggregateDF = sparkSession.sql(s"select count(*) numtasks, " +
      s"max(finishTime) - min(launchTime) as elapsedTime, sum(duration), sum(schedulerDelay), sum(executorRunTime), " +
      s"sum(executorCpuTime), sum(executorDeserializeTime), sum(executorDeserializeCpuTime), " +
      s"sum(resultSerializationTime), sum(jvmGCTime), sum(shuffleFetchWaitTime), sum(shuffleWriteTime), " +
      s"sum(gettingResultTime), " +
      s"max(resultSize), sum(numUpdatedBlockStatuses), sum(diskBytesSpilled), sum(memoryBytesSpilled), " +
      s"max(peakExecutionMemory), sum(recordsRead), sum(bytesRead), sum(recordsWritten), sum(bytesWritten), " +
      s" sum(shuffleTotalBytesRead), sum(shuffleTotalBlocksFetched), sum(shuffleLocalBlocksFetched), " +
      s"sum(shuffleRemoteBlocksFetched), sum(shuffleBytesWritten), sum(shuffleRecordsWritten) " +
      s"from PerfTaskMetrics " +
      s"where launchTime >= $beginSnapshot and finishTime <= $endSnapshot")

    result = result :+ (s"\nScheduling mode = ${sparkSession.sparkContext.getSchedulingMode.toString}")
    result = result :+ (s"Spark Contex default degree of parallelism = ${sparkSession.sparkContext.defaultParallelism}")
    result = result :+ ("Aggregated Spark task metrics:")

    /** Print a summary of the task metrics. */
    val aggregateValues = aggregateDF.take(1)(0).toSeq
    val cols = aggregateDF.columns
    result = result :+ ((cols zip aggregateValues)
      .map {
        case ((n: String, v: Long)) =>
          Utils.prettyPrintValues(n, v)
      }).mkString("\n")

    return (result.mkString("\n"))
  }

  def printReport(): Unit = {
    println(report)
  }

  /** Shortcut to run and measure the metrics for Spark execution, built after spark.time() */
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

  /** helper method to save data, we expect to have moderate amounts of data so collapsing to 1 partition seems OK */
  def saveData(df: DataFrame, fileName: String, fileFormat: String = "json") = {
    df.orderBy("jobId", "stageId", "index").repartition(1).write.format(fileFormat).save(fileName)
    logger.warn(s"Task metric data saved into $fileName using format=$fileFormat")
  }

}
