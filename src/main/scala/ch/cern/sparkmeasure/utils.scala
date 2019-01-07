package ch.cern.sparkmeasure

import scala.collection.mutable.ListBuffer
import java.io._
import java.nio.file.Paths

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
 * The object Utils contains some helper code for the sparkMeasure package
 * The methods formatDuration and formatBytes are used for printing stage metrics reports
 * The methods readSerializedStageMetrics and readSerializedTaskMetrics are used to read data serialized into files by
 * "flight recorder" mode
 */

object Utils {

  val objectMapper = new ObjectMapper with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)
  val objectWriter = objectMapper.writer(new DefaultPrettyPrinter())


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

  class ObjectInputStreamWithCustomClassLoader(fileInputStream: FileInputStream) extends ObjectInputStream(fileInputStream) {
    override def resolveClass(desc: ObjectStreamClass): Class[_] = {
      try {
        Class.forName(desc.getName, false, getClass.getClassLoader)
      }
      catch {
        case ex: ClassNotFoundException => super.resolveClass(desc)
      }
    }
  }

  def readSerialized[T](metricsFileName: String): ListBuffer[T] = {
    val fullPath = Paths.get(metricsFileName).toString
    val ois = new ObjectInputStreamWithCustomClassLoader(new FileInputStream(fullPath))
    try {
      ois.readObject().asInstanceOf[ListBuffer[T]]
    } finally {
      ois.close()
    }
  }

  def writeSerialized(fullPath: String, metricsData: Any): Unit = {
    val os = new ObjectOutputStream(new FileOutputStream(fullPath))
    try {
      os.writeObject(metricsData)
    } finally {
      os.close()
    }
  }

  def readSerializedStageMetrics(stageMetricsFileName: String): ListBuffer[StageVals] = {
    readSerialized[StageVals](stageMetricsFileName)
  }

  def readSerializedTaskMetrics(stageMetricsFileName: String): ListBuffer[TaskVals] = {
    readSerialized[TaskVals](stageMetricsFileName)
  }

  def writeSerializedJSON(fullPath: String, metricsData: AnyRef): Unit = {
    val os = new FileOutputStream(fullPath)
    try {
      objectWriter.writeValue(os, metricsData)
    } finally {
      os.close()
    }
  }

  def writeToStringSerializedJSON(metricsData: AnyRef): String = {
    objectWriter.writeValueAsString(metricsData)
  }

  def readSerializedStageMetricsJSON(stageMetricsFileName: String): List[StageVals] = {
    val fullPath = Paths.get(stageMetricsFileName).toString
    val is = new FileInputStream(fullPath)
    try {
      objectMapper.readValue[List[StageVals]](is)
    } finally {
      is.close()
    }
  }

  def readSerializedTaskMetricsJSON(taskMetricsFileName: String): List[TaskVals] = {
    val fullPath = Paths.get(taskMetricsFileName).toString
    val is = new FileInputStream(fullPath)
    try {
      objectMapper.readValue[List[TaskVals]](is)
    } finally {
      is.close()
    }
  }
}
