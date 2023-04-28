package ch.cern.sparkmeasure

import org.apache.spark.sql.SparkSession

import org.scalatest.{FunSuite, BeforeAndAfterAll}

import scala.util.Properties

class StageMetricsTest extends FunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    if (Properties.versionNumberString.startsWith("2.12")) {
      spark = SparkSession.builder()
        .master("local[*]")
        .appName("testSparkMeasure")
        .config("spark.jars", "target/scala-2.12/*.jar")
        .getOrCreate()
    } else if (Properties.versionNumberString.startsWith("2.13")) {
      spark = SparkSession.builder()
        .master("local[*]")
        .appName("testSparkMeasure")
        .config("spark.jars", "target/scala-2.13/*.jar")
        .getOrCreate()
    }
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("test StageMetrics") {
    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)
    stageMetrics.runAndMeasure {
      spark.sql("select count(*) from range(100) cross join range(100) cross join range(100)").show()
    }
    val metrics = stageMetrics.aggregateStageMetrics()
    assert(metrics("numStages") > 1)
  }

}
