package ch.cern.sparkmeasure

import org.slf4j.Logger
import org.apache.spark.SparkConf

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

    val (value, unit): (Double, String) = {
      if (bytes >= 2*trillion) {
        (bytes / trillion, " TB")
      } else if (bytes >= 2*billion) {
        (bytes / billion, " GB")
      } else if (bytes >= 2*million) {
        (bytes / million, " MB")
      } else if (bytes >= 2*thousand) {
        (bytes / thousand, " KB")
      } else {
        (bytes, " Bytes")
      }
    }
    if (unit == " Bytes") {
      "%d%s".format(value.toInt, unit)
    } else {
      "%.1f%s".format(value, unit)
    }
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
}
