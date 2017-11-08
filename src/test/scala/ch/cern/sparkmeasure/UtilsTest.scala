package ch.cern.sparkmeasure

import java.io.File

import scala.collection.mutable.ListBuffer

import org.scalatest.{FlatSpec, Matchers}

class UtilsTest extends FlatSpec with Matchers {

  val stageVals0 = StageVals(jobId = 1, stageId = 2, name = "stageVal",
    submissionTime = 10, completionTime = 11, stageDuration = 12, numTasks = 13,
    executorRunTime = 14, executorCpuTime = 15,
    executorDeserializeTime = 16, executorDeserializeCpuTime = 17,
    resultSerializationTime = 18, jvmGCTime = 19, resultSize = 20, numUpdatedBlockStatuses = 21,
    diskBytesSpilled = 30, memoryBytesSpilled = 31, peakExecutionMemory = 32, recordsRead = 33,
    bytesRead = 34, recordsWritten = 35, bytesWritten = 36,
    shuffleFetchWaitTime = 40, shuffleTotalBytesRead = 41, shuffleTotalBlocksFetched = 42,
    shuffleLocalBlocksFetched = 43, shuffleRemoteBlocksFetched = 44, shuffleWriteTime = 45,
    shuffleBytesWritten = 46, shuffleRecordsWritten = 47
  )

  val taskVals0 = TaskVals(jobId = 1, stageId = 2, index = 3, launchTime = 4, finishTime = 5,
    duration = 10, schedulerDelay = 11, executorId = "exec0", host = "host0", taskLocality = 12,
    speculative = false, gettingResultTime = 12, successful = true,
    executorRunTime = 14, executorCpuTime = 15,
    executorDeserializeTime = 16, executorDeserializeCpuTime = 17,
    resultSerializationTime = 18, jvmGCTime = 19, resultSize = 20, numUpdatedBlockStatuses = 21,
    diskBytesSpilled = 30, memoryBytesSpilled = 31, peakExecutionMemory = 32, recordsRead = 33,
    bytesRead = 34, recordsWritten = 35, bytesWritten = 36,
    shuffleFetchWaitTime = 40, shuffleTotalBytesRead = 41, shuffleTotalBlocksFetched = 42,
    shuffleLocalBlocksFetched = 43, shuffleRemoteBlocksFetched = 44, shuffleWriteTime = 45,
    shuffleBytesWritten = 46, shuffleRecordsWritten = 47
  )

  it should "write and read back StageVal (Java Serialization)" in {
    val file = File.createTempFile("stageVal", ".tmp")
    try {
      Utils.writeSerialized(file.getAbsolutePath, ListBuffer(stageVals0))
      val stageVals = Utils.readSerializedStageMetrics(file.getAbsolutePath)
      stageVals should have length 1
      stageVals.head shouldEqual stageVals0
    } finally {
      file.delete()
    }
  }

  it should "write and read back TaskVal (Java Serialization)" in {
    val file = File.createTempFile("taskVal", ".tmp")
    try {
      Utils.writeSerialized(file.getAbsolutePath, ListBuffer(taskVals0))
      val taskVals = Utils.readSerializedTaskMetrics(file.getAbsolutePath)
      taskVals should have length 1
      taskVals.head shouldEqual taskVals0
    } finally {
      file.delete()
    }
  }

  it should "write and read back StageVal JSON" in {
    val file = File.createTempFile("stageVal", ".json")
    try {
      Utils.writeSerializedJSON(file.getAbsolutePath, ListBuffer(stageVals0))
      val stageVals = Utils.readSerializedStageMetricsJSON(file.getAbsolutePath)
      stageVals should have length 1
      stageVals.head shouldEqual stageVals0
    } finally {
      file.delete()
    }
  }

  it should "write and read back TaskVal JSON" in {
    val file = File.createTempFile("taskVal", ".json")
    try {
      Utils.writeSerializedJSON(file.getAbsolutePath, ListBuffer(taskVals0))
      val taskVals = Utils.readSerializedTaskMetricsJSON(file.getAbsolutePath)
      taskVals should have length 1
      taskVals.head shouldEqual taskVals0
    } finally {
      file.delete()
    }
  }

}
