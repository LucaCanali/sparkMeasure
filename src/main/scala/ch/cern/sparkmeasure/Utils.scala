package ch.cern.sparkmeasure

import org.slf4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.TaskLocality

import scala.collection.mutable.LinkedHashMap

/**
 * The object Utils contains some helper code for the sparkMeasure package
 * The methods formatDuration and formatBytes are used for printing stage metrics reports
 */

object Utils {

  /** boilerplate code for pretty printing, formatDuration code borrowed from Spark UIUtils */
  def formatDuration(milliseconds: Long): String = {
    if (milliseconds < 100) {
      return "%d ms".format(milliseconds)
    }
    val seconds = milliseconds.toDouble / 1000
    if (seconds < 1) {
      return "%.1f s".format(seconds)
    }
    if (seconds < 60) {
      return "%.0f s".format(seconds)
    }
    val minutes = seconds / 60
    if (minutes < 10) {
      return "%.1f min".format(minutes)
    } else if (minutes < 60) {
      return "%.0f min".format(minutes)
    }
    val hours = minutes / 60
    "%.1f h".format(hours)
  }

  def formatBytes(bytes: Long): String = {
    val trillion = 1024L*1024L*1024L*1024L
    val billion = 1024L*1024L*1024L
    val million = 1024L*1024L
    val thousand = 1024L
    val bytesDouble = bytes.toDouble

    val (value, unit): (Double, String) = {
      if (bytesDouble >= 2*trillion) {
        (bytesDouble / trillion, " TB")
      } else if (bytes >= 2*billion) {
        (bytesDouble / billion, " GB")
      } else if (bytes >= 2*million) {
        (bytesDouble / million, " MB")
      } else if (bytes >= 2*thousand) {
        (bytesDouble / thousand, " KB")
      } else {
        (bytesDouble, " Bytes")
      }
    }
    if (unit == " Bytes") {
      "%d%s".format(value.toInt, unit)
    } else {
      "%.1f%s".format(value, unit)
    }
  }

  // Return the data structure use to compute metrics reports for StageMetrics
  def zeroMetricsStage() : LinkedHashMap[String, Long] = {
    val zeroedMetrics = LinkedHashMap(
      "numStages" -> 0L,
      "numTasks" -> 0L,
      "elapsedTime" -> 0L,
      "stageDuration" -> 0L,
      "executorRunTime" -> 0L,
      "executorCpuTime" -> 0L,
      "executorDeserializeTime" -> 0L,
      "executorDeserializeCpuTime" -> 0L,
      "resultSerializationTime" -> 0L,
      "jvmGCTime" -> 0L,
      "shuffleFetchWaitTime"-> 0L,
      "shuffleWriteTime"-> 0L,
      "resultSize" -> 0L,
      "diskBytesSpilled" -> 0L,
      "memoryBytesSpilled" -> 0L,
      "peakExecutionMemory" -> 0L,
      "recordsRead" -> 0L,
      "bytesRead" -> 0L,
      "recordsWritten" -> 0L,
      "bytesWritten" -> 0L,
      "shuffleRecordsRead" -> 0L,
      "shuffleTotalBlocksFetched" -> 0L,
      "shuffleLocalBlocksFetched" -> 0L,
      "shuffleRemoteBlocksFetched" -> 0L,
      "shuffleTotalBytesRead" -> 0L,
      "shuffleLocalBytesRead" -> 0L,
      "shuffleRemoteBytesRead" -> 0L,
      "shuffleRemoteBytesReadToDisk" -> 0L,
      "shuffleBytesWritten" -> 0L,
      "shuffleRecordsWritten" -> 0L
    )
    zeroedMetrics
  }

  // Return the data structure use to compute metrics reports for TaskMetrics
  def zeroMetricsTask() : LinkedHashMap[String, Long] = {
    val zeroedMetrics = LinkedHashMap(
      "numTasks" -> 0L,
      "successful tasks" -> 0L,
      "speculative tasks" -> 0L,
      "taskDuration" -> 0L,
      "schedulerDelayTime" -> 0L,
      "executorRunTime" -> 0L,
      "executorCpuTime" -> 0L,
      "executorDeserializeTime" -> 0L,
      "executorDeserializeCpuTime" -> 0L,
      "resultSerializationTime" -> 0L,
      "jvmGCTime" -> 0L,
      "shuffleFetchWaitTime"-> 0L,
      "shuffleWriteTime" -> 0L,
      "gettingResultTime" -> 0L,
      "resultSize" -> 0L,
      "diskBytesSpilled" -> 0L,
      "memoryBytesSpilled" -> 0L,
      "peakExecutionMemory" -> 0L,
      "recordsRead" -> 0L,
      "bytesRead" -> 0L,
      "recordsWritten" -> 0L,
      "bytesWritten" -> 0L,
      "shuffleRecordsRead" -> 0L,
      "shuffleTotalBlocksFetched" -> 0L,
      "shuffleLocalBlocksFetched" -> 0L,
      "shuffleRemoteBlocksFetched" -> 0L,
      "shuffleTotalBytesRead" -> 0L,
      "shuffleLocalBytesRead" -> 0L,
      "shuffleRemoteBytesRead" -> 0L,
      "shuffleRemoteBytesReadToDisk" -> 0L,
      "shuffleBytesWritten" -> 0L,
      "shuffleRecordsWritten" -> 0L
    )
    zeroedMetrics
  }

  def prettyPrintValues(metric: String, value: Long): String = {
    val name = metric.toLowerCase()
    val basicValue = value.toString
    val optionalValueWithUnits = {
      if (name.contains("time") || name.contains("duration")) {
        " (" + formatDuration(value) + ")"
      }
      else if (name.contains("bytes") || name.contains("size")) {
        " (" + formatBytes(value) + ")"
      }
      else ""
    }
    metric + " => " + basicValue + optionalValueWithUnits
  }

  def encodeTaskLocality(taskLocality: TaskLocality.TaskLocality): Int = {
    taskLocality match {
      case TaskLocality.PROCESS_LOCAL => 0
      case TaskLocality.NODE_LOCAL => 1
      case TaskLocality.RACK_LOCAL => 2
      case TaskLocality.NO_PREF => 3
      case TaskLocality.ANY => 4
      case _ => -1 // Flag an unknown situation
     }
  }

  // handle metrics format parameter
  def parseMetricsFormat(conf: SparkConf, logger: Logger, defaultFormat:String) : String = {
    // handle metrics format parameter
    val metricsFormat = conf.get("spark.sparkmeasure.outputFormat", defaultFormat)
    metricsFormat match {
      case "json" | "java" | "json_to_hadoop" =>
        logger.info(s"Using $metricsFormat as serialization format.")
      case _ => logger.warn(s"Invalid serialization format: $metricsFormat." +
        " Configure with: spark.sparkmeasure.outputFormat=json|javaser|json_to_hadoop")
    }
    metricsFormat
  }

  def parsePrintToStdout(conf: SparkConf, logger: Logger, defaultVal:Boolean) : Boolean = {
    val printToStdout = conf.getBoolean("spark.sparkmeasure.printToStdout", defaultVal)
    if (printToStdout == true) {
      logger.info(s"Will print metrics output to stdout in JSON format")
    }
    printToStdout
  }

  // handle metrics file name parameter except if writing to string / stdout
  def parseMetricsFilename(conf: SparkConf, logger: Logger, defaultFileName:String) : String = {
    val metricsFileName = conf.get("spark.sparkmeasure.outputFilename", defaultFileName)
    if (metricsFileName.isEmpty) {
      logger.warn("No output file will be written. If you want to write the output to a file, " +
        "configure with spark.sparkmeasure.outputFilename=<output file>")
    } else {
      logger.info(s"Writing output to $metricsFileName")
    }
    metricsFileName
  }

  def parseInfluxDBURL(conf: SparkConf, logger: Logger) : String = {
    // handle InfluxDB URL
    val influxdbURL = conf.get("spark.sparkmeasure.influxdbURL", "http://localhost:8086")
    if (influxdbURL.isEmpty) {
      logger.error("InfluxDB URL not found, this will make the listener fail.")
      throw new RuntimeException
    } else {
      logger.info(s"Found URL for InfluxDB: $influxdbURL")
    }
    influxdbURL
  }

  def parseInfluxDBCredentials(conf: SparkConf, logger: Logger) : (String,String) = {
    // handle InfluxDB username and password
    val influxdbUsername = conf.get("spark.sparkmeasure.influxdbUsername", "")
    val influxdbPassword = conf.get("spark.sparkmeasure.influxdbPassword", "")
    if (influxdbUsername.isEmpty && influxdbPassword.isEmpty) {
      logger.warn("Credentials for InfluxDB connection not found, using empty username and password, " +
        "InfluxDB must be running with auth-enabled=false")
    } else {
      logger.info("Credentials for InfluxDB connection found")
    }
    (influxdbUsername, influxdbPassword)
  }

  def parseInfluxDBName(conf: SparkConf, logger: Logger) : String = {
    // handle InfluxDB username and password
    val influxdbName = conf.get("spark.sparkmeasure.influxdbName", "sparkmeasure")
    logger.info(s"InfluxDB name: $influxdbName")
    influxdbName
  }

  def parseInfluxDBStagemetrics(conf: SparkConf, logger: Logger) : Boolean = {
    // handle InfluxDB username and password
    val influxdbStagemetrics = conf.getBoolean("spark.sparkmeasure.influxdbStagemetrics", false)
    logger.info(s"Log also stagemetrics: ${influxdbStagemetrics.toString}")
    influxdbStagemetrics
  }

  def parseKafkaConfig(conf: SparkConf, logger: Logger) : (String,String) = {
    // handle InfluxDB username and password
    val broker = conf.get("spark.sparkmeasure.kafkaBroker", "")
    val topic = conf.get("spark.sparkmeasure.kafkaTopic", "")
    if (broker.isEmpty || topic.isEmpty) {
      throw new IllegalArgumentException("broker & topic are required for the Kafka connection")
    } else {
      logger.info(s"Kafka broker: $broker")
      logger.info(s"Kafka topic: $topic")
    }
    (broker, topic)
  }

  // handle list of metrics to process by the listener onExecutorMetricsUpdate
  // returns an array with the metrics to process
  def parseExecutorMetricsConfig(conf: SparkConf, logger: Logger) : Array[String] = {
    val metrics = conf.get("spark.sparkmeasure.stageinfo.executormetrics",
      "JVMHeapMemory,OnHeapExecutionMemory")
    logger.info(s"Executor metrics being collected: ${metrics.toString}")
    metrics.split(",", -1)
  }

  // boolean flag, turns on/off collecting and reporting additional stage info (duration and memory details)
  def parseExtraStageMetrics(conf: SparkConf, logger: Logger) : Boolean = {
    // handle InfluxDB username and password
    val extraStageMetrics = conf.getBoolean("spark.sparkmeasure.stageinfo.verbose", true)
    logger.info(s"Collect and report extra stage metrics: ${extraStageMetrics.toString}")
    extraStageMetrics
  }

}
