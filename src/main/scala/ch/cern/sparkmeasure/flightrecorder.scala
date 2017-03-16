package ch.cern.sparkmeasure

import java.io.{FileOutputStream, ObjectOutputStream}
import java.nio.file.Paths

import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SparkListenerApplicationEnd

/**
  * Created by luca on 3/15/17.
  */

class FlightRecorderStageMetrics(conf: SparkConf) extends StageInfoRecorderListener {

  lazy val logger = LogManager.getLogger("flightrecorder")

  val metricsFileName = conf.get("spark.executorEnv.stageMetricsFileName", "/tmp/stageMetrics.serialized")
  val fullPath = Paths.get(metricsFileName).toString

  /** when the application stops serialize the content of stageMetricsData into a file in the driver's filesystem */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logger.warn(s"application end, timestmap = ${applicationEnd.time}")

    val os = new ObjectOutputStream(new FileOutputStream(fullPath))
    os.writeObject(stageMetricsData)
    os.close()

    logger.warn(s"Stagemetrics data serialized to $fullPath")
  }
}

class FlightRecorderTaskMetrics(conf: SparkConf) extends TaskInfoRecorderListener {

  lazy val logger = LogManager.getLogger("flightrecorder")

  val metricsFileName = conf.get("spark.executorEnv.taskMetricsFileName", "/tmp/taskMetrics.serialized")
  val fullPath = Paths.get(metricsFileName).toString

  /** when the application stops serialize the content of taskMetricsData into a file in the driver's filesystem */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logger.warn(s"application end, timestmap = ${applicationEnd.time}")

    val os = new ObjectOutputStream(new FileOutputStream(fullPath))
    os.writeObject(taskMetricsData)
    os.close()

    logger.warn(s"Taskmetrics data serialized to $fullPath")
  }
}
