package ch.cern.sparkmeasure

import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ListBuffer
import scala.util.Properties

/**
 * Test suite for KafkaSinkV2 to verify:
 * 1. Application start/end event schema and payload correctness
 * 2. Counter tracking (jobs, stages, tasks, executors)
 * 3. Custom labels extraction and inclusion
 * 4. Spark configurations extraction
 * 5. Backward compatibility with KafkaSink events
 */
class KafkaSinkV2Test extends AnyFunSuite with BeforeAndAfterAll with Matchers {

  var spark: SparkSession = _
  val capturedEvents: ListBuffer[Map[String, Any]] = ListBuffer.empty

  class TestableKafkaSinkV2(conf: SparkConf) extends KafkaSinkV2(conf) {
    override protected def report[T <: Any](metrics: Map[String, T]): Unit = {
      // Capture events for testing instead of sending to Kafka
      capturedEvents += metrics.asInstanceOf[Map[String, Any]]
    }
  }

  override def beforeAll(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("KafkaSinkV2Test")
      .set("spark.sparkmeasure.kafkaBroker", "test:9092")
      .set("spark.sparkmeasure.kafkaTopic", "test-topic")
      .set("spark.sparkmeasure.appLabels.project", "test-project")
      .set("spark.sparkmeasure.appLabels.environment", "test-env")
      .set("spark.sparkmeasure.appLabels.team", "test-team")
      .set("spark.executor.instances", "2")
      .set("spark.executor.memory", "1g")
      .set("spark.sql.shuffle.partitions", "10")

    if (Properties.versionNumberString.startsWith("2.12")) {
      spark = SparkSession.builder()
        .config(conf)
        .config("spark.jars", "target/scala-2.12/*.jar")
        .getOrCreate()
    } else if (Properties.versionNumberString.startsWith("2.13")) {
      spark = SparkSession.builder()
        .config(conf)
        .config("spark.jars", "target/scala-2.13/*.jar")
        .getOrCreate()
    }
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("applications_started event should contain correct schema and custom labels") {
    capturedEvents.clear()
    val listener = new TestableKafkaSinkV2(spark.sparkContext.getConf)

    val appStartEvent = SparkListenerApplicationStart(
      appName = "TestApp",
      appId = Some("test-app-001"),
      time = 1234567890L,
      sparkUser = "testuser",
      appAttemptId = None,
      driverLogs = None
    )

    listener.onApplicationStart(appStartEvent)

    val startEvents = capturedEvents.filter(_("name") == "applications_started")
    assert(startEvents.nonEmpty, "applications_started event should be emitted")

    val startEvent = startEvents.head
    assert(startEvent("appId") == "test-app-001", "appId should match")
    assert(startEvent("appName") == "TestApp", "appName should match")
    assert(startEvent("startTime") == 1234567890L, "startTime should match")
    assert(startEvent.contains("epochMillis"), "epochMillis should be present")

    // Verify custom labels are included
    assert(startEvent("appLabels.project") == "test-project", "project label should be present")
    assert(startEvent("appLabels.environment") == "test-env", "environment label should be present")
    assert(startEvent("appLabels.team") == "test-team", "team label should be present")
  }

  test("applications_ended event should contain basic structure and custom labels") {
    capturedEvents.clear()
    val listener = new TestableKafkaSinkV2(spark.sparkContext.getConf)

    val appStartEvent = SparkListenerApplicationStart(
      appName = "TestApp",
      appId = Some("test-app-002"),
      time = 1000000000L,
      sparkUser = "testuser",
      appAttemptId = None,
      driverLogs = None
    )
    listener.onApplicationStart(appStartEvent)

    val appEndEvent = SparkListenerApplicationEnd(time = 1000010000L)
    listener.onApplicationEnd(appEndEvent)

    val endEvents = capturedEvents.filter(_("name") == "applications_ended")
    assert(endEvents.nonEmpty, "applications_ended event should be emitted")

    val endEvent = endEvents.head

    // Verify basic fields
    assert(endEvent("appId") == "test-app-002", "appId should match")
    assert(endEvent("appName") == "TestApp", "appName should match")
    assert(endEvent("startTime") == 1000000000L, "startTime should match")
    assert(endEvent("completionTime") == 1000010000L, "completionTime should match")
    assert(endEvent("duration") == 10000L, "duration should be calculated correctly")
    assert(endEvent.contains("epochMillis"), "epochMillis should be present")

    // Verify counters are initialized
    assert(endEvent("totalJobsCompleted") == 0, "should have 0 jobs initially")
    assert(endEvent("succeededJobsCount") == 0, "should have 0 succeeded jobs")
    assert(endEvent("failedJobsCount") == 0, "should have 0 failed jobs")
    assert(endEvent("numStagesCompleted") == 0, "should have 0 stages")
    assert(endEvent("totalTaskCount") == 0, "should have 0 tasks")
    assert(endEvent("executorsFailed") == 0, "should have 0 failed executors")
    assert(endEvent("executorsKilled") == 0, "should have 0 killed executors")

    // Verify custom labels
    assert(endEvent("appLabels.project") == "test-project", "project label should be present")
    assert(endEvent("appLabels.environment") == "test-env", "environment label should be present")
    assert(endEvent("appLabels.team") == "test-team", "team label should be present")

    // Verify spark configurations are captured
    assert(endEvent.contains("sparkConfigs"), "sparkConfigs should be present")
    val sparkConfigs = endEvent("sparkConfigs").asInstanceOf[Map[String, String]]
    assert(sparkConfigs.contains("spark.executor.instances"), "executor.instances should be captured")
    assert(sparkConfigs("spark.executor.instances") == "2", "executor.instances value should match")
    assert(sparkConfigs.contains("spark.executor.memory"), "executor.memory should be captured")
    assert(sparkConfigs("spark.executor.memory") == "1g", "executor.memory value should match")
    assert(sparkConfigs.contains("spark.sql.shuffle.partitions"), "shuffle.partitions should be captured")
    assert(sparkConfigs("spark.sql.shuffle.partitions") == "10", "shuffle.partitions value should match")
  }

  test("job counters should track succeeded and failed jobs") {
    capturedEvents.clear()
    val listener = new TestableKafkaSinkV2(spark.sparkContext.getConf)

    val appStartEvent = SparkListenerApplicationStart(
      appName = "TestApp",
      appId = Some("test-app-003"),
      time = 1000000000L,
      sparkUser = "testuser",
      appAttemptId = None,
      driverLogs = None
    )
    listener.onApplicationStart(appStartEvent)

    // Simulate successful job
    val jobStart1 = SparkListenerJobStart(
      jobId = 1,
      time = 1000001000L,
      stageInfos = Seq(),
      properties = null
    )
    listener.onJobStart(jobStart1)

    val jobEnd1 = SparkListenerJobEnd(
      jobId = 1,
      time = 1000002000L,
      jobResult = org.apache.spark.scheduler.JobSucceeded
    )
    listener.onJobEnd(jobEnd1)

    // Simulate another successful job
    val jobStart2 = SparkListenerJobStart(
      jobId = 2,
      time = 1000003000L,
      stageInfos = Seq(),
      properties = null
    )
    listener.onJobStart(jobStart2)

    val jobEnd2 = SparkListenerJobEnd(
      jobId = 2,
      time = 1000004000L,
      jobResult = org.apache.spark.scheduler.JobSucceeded
    )
    listener.onJobEnd(jobEnd2)

    val appEndEvent = SparkListenerApplicationEnd(time = 1000010000L)
    listener.onApplicationEnd(appEndEvent)

    val endEvents = capturedEvents.filter(_("name") == "applications_ended")
    val endEvent = endEvents.head

    assert(endEvent("totalJobsCompleted") == 2, "should have 2 total jobs")
    assert(endEvent("succeededJobsCount") == 2, "should have 2 succeeded jobs")
    assert(endEvent("failedJobsCount") == 0, "should have 0 failed jobs")
  }

  test("executor removal tracking should work") {
    capturedEvents.clear()
    val listener = new TestableKafkaSinkV2(spark.sparkContext.getConf)

    val appStartEvent = SparkListenerApplicationStart(
      appName = "TestApp",
      appId = Some("test-app-004"),
      time = 1000000000L,
      sparkUser = "testuser",
      appAttemptId = None,
      driverLogs = None
    )
    listener.onApplicationStart(appStartEvent)

    // Simulate killed executor
    val executorRemovedKilled = SparkListenerExecutorRemoved(
      time = 1000001000L,
      executorId = "exec-1",
      reason = "Executor killed by driver"
    )
    listener.onExecutorRemoved(executorRemovedKilled)

    // Simulate failed executor
    val executorRemovedFailed = SparkListenerExecutorRemoved(
      time = 1000002000L,
      executorId = "exec-2",
      reason = "Executor lost due to node failure"
    )
    listener.onExecutorRemoved(executorRemovedFailed)

    val appEndEvent = SparkListenerApplicationEnd(time = 1000010000L)
    listener.onApplicationEnd(appEndEvent)

    val endEvents = capturedEvents.filter(_("name") == "applications_ended")
    val endEvent = endEvents.head

    assert(endEvent("executorsKilled") == 1, "should have 1 killed executor")
    assert(endEvent("executorsFailed") == 1, "should have 1 failed executor")
  }

  test("backward compatibility - KafkaSinkV2 should emit KafkaSink event types") {
    capturedEvents.clear()
    val listener = new TestableKafkaSinkV2(spark.sparkContext.getConf)

    val appStartEvent = SparkListenerApplicationStart(
      appName = "TestApp",
      appId = Some("test-app-005"),
      time = 1000000000L,
      sparkUser = "testuser",
      appAttemptId = None,
      driverLogs = None
    )
    listener.onApplicationStart(appStartEvent)

    // Job events
    val jobStart = SparkListenerJobStart(
      jobId = 1,
      time = 1000001000L,
      stageInfos = Seq(),
      properties = null
    )
    listener.onJobStart(jobStart)

    val jobStartedEvents = capturedEvents.filter(_("name") == "jobs_started")
    assert(jobStartedEvents.nonEmpty, "jobs_started event should be emitted (backward compatibility)")
    assert(jobStartedEvents.head("jobId") == "1", "jobId should match")

    val jobEnd = SparkListenerJobEnd(
      jobId = 1,
      time = 1000002000L,
      jobResult = org.apache.spark.scheduler.JobSucceeded
    )
    listener.onJobEnd(jobEnd)

    val jobEndedEvents = capturedEvents.filter(_("name") == "jobs_ended")
    assert(jobEndedEvents.nonEmpty, "jobs_ended event should be emitted (backward compatibility)")
  }

  test("custom labels extraction should handle missing labels gracefully") {
    val conf = new SparkConf()
      .set("spark.sparkmeasure.kafkaBroker", "test:9092")
      .set("spark.sparkmeasure.kafkaTopic", "test-topic")
    // No custom labels set

    capturedEvents.clear()
    val listener = new TestableKafkaSinkV2(conf)

    val appStartEvent = SparkListenerApplicationStart(
      appName = "TestApp",
      appId = Some("test-app-006"),
      time = 1234567890L,
      sparkUser = "testuser",
      appAttemptId = None,
      driverLogs = None
    )

    listener.onApplicationStart(appStartEvent)

    val startEvents = capturedEvents.filter(_("name") == "applications_started")
    val startEvent = startEvents.head

    // Verify no custom label keys exist
    val customLabelKeys = startEvent.keys.filter(_.startsWith("appLabels."))
    assert(customLabelKeys.isEmpty, "no custom labels should be present when not configured")
  }

  test("integration test - run actual Spark query and verify metrics") {
    capturedEvents.clear()
    val listener = new TestableKafkaSinkV2(spark.sparkContext.getConf)
    
    // Add listener to spark context
    spark.sparkContext.addSparkListener(listener)

    // Run a simple query
    spark.sql("select count(*) from range(100)").collect()

    // Remove listener
    spark.sparkContext.removeSparkListener(listener)

    // Verify that standard KafkaSink events were emitted
    val jobStartedEvents = capturedEvents.filter(_("name") == "jobs_started")
    assert(jobStartedEvents.nonEmpty, "jobs_started events should be emitted during query execution")

    val jobEndedEvents = capturedEvents.filter(_("name") == "jobs_ended")
    assert(jobEndedEvents.nonEmpty, "jobs_ended events should be emitted during query execution")

    val stageStartedEvents = capturedEvents.filter(_("name") == "stages_started")
    assert(stageStartedEvents.nonEmpty, "stages_started events should be emitted during query execution")

    val stageEndedEvents = capturedEvents.filter(_("name") == "stages_ended")
    assert(stageEndedEvents.nonEmpty, "stages_ended events should be emitted during query execution")
  }
}
