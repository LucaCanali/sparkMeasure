package ch.cern.sparkmeasure

import scala.collection.mutable.ListBuffer
import java.io.{FileInputStream, ObjectInputStream, ObjectStreamClass}
import java.nio.file.Paths

/**
 * The object Utils contains some helper code for the sparkMeasure package
 * The methods formatDuration and formatBytes are used for printing stage metrics reports
 * The methods readSerializedStageMetrics and readSerializedTaskMetrics are used to read data serialized into files by
 * "flight recorder" mode
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
    val trillion = 1e12
    val billion = 1e9
    val million = 1e6
    val thousand = 1e3

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
        (bytes, "")
      }
    }
    if (unit.isEmpty) {
      "%d".format(value.toInt)
    } else {
      "%.1f%s".format(value, unit)
    }
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

  def readSerialized[T](stageMetricsFileName: String): ListBuffer[T] = {

    val fullPath = Paths.get(stageMetricsFileName).toString
    val ois = new ObjectInputStreamWithCustomClassLoader(new FileInputStream(fullPath))
    val result = ois.readObject().asInstanceOf[ListBuffer[T]]
    result
  }

  def readSerializedStageMetrics(stageMetricsFileName: String): ListBuffer[StageVals] = {
    readSerialized[StageVals](stageMetricsFileName)
  }

  def readSerializedTaskMetrics(stageMetricsFileName: String): ListBuffer[TaskVals] = {
    readSerialized[TaskVals](stageMetricsFileName)
  }

}
