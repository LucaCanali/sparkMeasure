package ch.cern.sparkmeasure

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.slf4j.LoggerFactory

/**
 * FlightRecorderStageMetrics - Use Spark Listeners defined in stagemetrics.scala to record task metrics data
 *                              aggregated at the Stage level, without changing the application code.
  *                             The resulting data can be saved to a file and/or printed to stdout.
 *
 * Use:
 *  by adding the following configuration to spark-submit (or Spark Session) configuration
 * --conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics
 *
 * Additional configuration parameters:
 * --conf spark.sparkmeasure.outputFormat=<format>, valid values: java,json,json_to_hadoop default "json"
 *   note: json and java serialization formats, write to the driver local filesystem
 *   json_to_hadoop, writes to JSON serialized metrics to  HDFS or to an Hadoop compliant filesystem, such as s3a
 *
 * --conf spark.sparkmeasure.outputFilename=<output file>, default: "/tmp/stageMetrics_flightRecorder"
 * --conf spark.sparkmeasure.printToStdout=<true|false>, default false. Set to true to print JSON serialized metrics
 *                                                       to stdout.
 */

class FlightRecorderStageMetrics(conf: SparkConf) extends
  StageInfoRecorderListener(false, Array.empty[String]) {
  // Note no extra Spark executor (memory) metrics collected on FlightRecorder for the FIle Sink

  lazy val logger = LoggerFactory.getLogger(this.getClass.getName)

  // handle metrics format parameter spark.sparkmeasure.outputFormat
  val metricsFormat = Utils.parseMetricsFormat(conf, logger, "json")

  // handle metrics format parameter spark.sparkmeasure.outputFilename
  val metricsFilename = Utils.parseMetricsFilename(conf, logger, "/tmp/stageMetrics_flightRecorder")

  // should we print to stdout too? handle conf: spark.sparkmeasure.printToStdout
  val printToStdout = Utils.parsePrintToStdout(conf, logger, false)

  // when the application stops, serialize the content of stageMetricsData into a file and/or print to stdout
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logger.info(s"Spark application ended, timestamp = ${applicationEnd.time}")

    if (printToStdout) {
      logger.info(s"Printing Stage metrics values serialized to JSON to stdout")
      val stringMetrics = IOUtils.writeToStringSerializedJSON(metricsFilename, stageMetricsData)
      print("Stage metrics values serialized to JSON:")
      print(stringMetrics)
    }

    if (metricsFilename.isEmpty) {
      logger.warn(("No Stage Metrics output file written."))
    } else {
      metricsFormat match {
        case "json" =>
          logger.warn(s"Writing Stage Metrics data serialized as json to $metricsFilename")
          IOUtils.writeSerializedJSON(metricsFilename, stageMetricsData)
        case "java" =>
          logger.warn(s"Writing Stage Metrics data serialized with java serialization to $metricsFilename")
          IOUtils.writeSerialized(metricsFilename, stageMetricsData)
        case "json_to_hadoop" =>
          logger.warn(s"Writing Stage Metrics data with json serialization to Hadoop filesystem to $metricsFilename")
          IOUtils.writeSerializedJSONToHadoop(metricsFilename, stageMetricsData, conf)
        case _ => logger.warn(s"StageMetrics data could not be written")
      }
    }
  }
}

/**
 * FlightRecorderTaskMetrics - Use a Spark Listener to record task metrics data and save them to a file
 *
 * Use:
 *  by adding the following configuration to spark-submit (or Spark Session) configuration
 * --conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderTaskMetrics
 *
 * Additional configuration parameters:
 * --conf spark.sparkmeasure.outputFormat=<format>, valid values: java,json,json_to_hadoop default "json"
 *   note: json and java serialization formats, write to the driver local filesystem
 *   json_to_hadoop, writes to JSON serialized metrics to  HDFS or to an Hadoop compliant filesystem, such as s3a
 *
 * --conf spark.sparkmeasure.outputFilename=<output file>, default: "/tmp/taskMetrics_flightRecorder"
 * --conf spark.sparkmeasure.printToStdout=<true|false>, default false. Set to true to print JSON serialized metrics
 *                                                       to stdout.
 */
class FlightRecorderTaskMetrics(conf: SparkConf) extends TaskInfoRecorderListener {

  lazy val logger = LoggerFactory.getLogger(this.getClass.getName)

  // handle metrics format parameter spark.sparkmeasure.outputFormat
  val metricsFormat = Utils.parseMetricsFormat(conf, logger, "json")

  // handle metrics format parameter spark.sparkmeasure.outputFilename
  val metricsFilename = Utils.parseMetricsFilename(conf, logger, "/tmp/taskMetrics_flightRecorder")

  // should we print to stdout too? handle conf: spark.sparkmeasure.printToStdout
  val printToStdout = Utils.parsePrintToStdout(conf, logger, false)

  // when the application stops, serialize the content of stageMetricsData into a file and/or print to stdout
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logger.info(s"Spark application ended, timestamp = ${applicationEnd.time}")

    if (printToStdout) {
      logger.info(s"Printing Task metrics values serialized to JSON to stdout")
      val stringMetrics = IOUtils.writeToStringSerializedJSON(metricsFilename, taskMetricsData)
      print("Task metrics values serialized to JSON:")
      print(stringMetrics)
    }

    if (metricsFilename.isEmpty) {
      logger.warn(("No Task Metrics output file written."))
    } else {
      metricsFormat match {
        case "json" =>
          logger.info(s"Writing Task Metrics data serialized as json to $metricsFilename")
          IOUtils.writeSerializedJSON(metricsFilename, taskMetricsData)
        case "java" =>
          logger.info(s"Writing Task Metrics data serialized with java serialization to $metricsFilename")
          IOUtils.writeSerialized(metricsFilename, taskMetricsData)
        case "json_to_hadoop" =>
          logger.warn(s"Writing Task Metrics data with json serialization to Hadoop filesystem to $metricsFilename")
          IOUtils.writeSerializedJSONToHadoop(metricsFilename, taskMetricsData, conf)
        case _ => logger.warn(s"StageMetrics data could not be written")
      }
    }
  }
}
