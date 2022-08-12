package ch.cern.sparkmeasure

import java.io._
import java.nio.file.Paths

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule,ScalaObjectMapper}

import org.apache.spark.SparkConf

import scala.collection.mutable.ListBuffer

/**
 * The object IOUtils contains some helper code for the sparkMeasure package
 * The methods readSerializedStageMetrics and readSerializedTaskMetrics are used to read data serialized into files by
 * the "flight recorder" mode.
 * Two serialization modes are supported currently: java serialization and JSON serialization with jackson library.
 */
object IOUtils {

  val objectMapper = new ObjectMapper with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)
  val objectWriter = objectMapper.writer(new DefaultPrettyPrinter())

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
    val fullPath = metricsFileName
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

  def writeSerializedJSONToHadoop(fullPath: String, metricsData: AnyRef, conf: SparkConf): Unit = {
    val fullPathUri = java.net.URI.create(fullPath)
    val hdfswritepath = new org.apache.hadoop.fs.Path(fullPath)

    // handle Hadoop configuration, for HDFS, S3A and/or other Hadoop compliant file systems, if configured
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    // additional s3 specific settings:
    val keyId = System.getenv("AWS_ACCESS_KEY_ID")
    val accessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
    if (keyId != null && accessKey != null) {
      hadoopConf.set("fs.s3.awsAccessKeyId", keyId)
      hadoopConf.set("fs.s3n.awsAccessKeyId", keyId)
      hadoopConf.set("fs.s3a.access.key", keyId)
      hadoopConf.set("fs.s3.awsSecretAccessKey", accessKey)
      hadoopConf.set("fs.s3n.awsSecretAccessKey", accessKey)
      hadoopConf.set("fs.s3a.secret.key", accessKey)

      val sessionToken = System.getenv("AWS_SESSION_TOKEN")
      if (sessionToken != null) {
        hadoopConf.set("fs.s3a.session.token", sessionToken)
      }
    }
    // add Spark configuration to hadoop configuration
    for ((key, value) <- conf.getAll if key.startsWith("spark.hadoop.")) {
      hadoopConf.set(key.substring("spark.hadoop.".length), value)
    }

    // get the Data in JSON format into a string
    val serializedMetrics = writeToStringSerializedJSON(metricsData)

    // write to the Hadoop filesystem
    val fs = org.apache.hadoop.fs.FileSystem.get(fullPathUri, hadoopConf)
    val outputStream = fs.create(hdfswritepath)
    try {
      outputStream.writeBytes(serializedMetrics)
    } finally {
      outputStream.close
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
