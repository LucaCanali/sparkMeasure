/****************************************
 *  sparkMeasure – build definition     *
 ****************************************/

name := "spark-measure"

version := "0.28-SNAPSHOT"

scalaVersion       := "2.13.18"

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

// ─── Dependencies ─────────────────────────────────────────────────────────────
val testDeps = Seq(
  "org.scalatest" %% "scalatest"                % "3.2.19" % Test,
  "org.scalatest" %% "scalatest-shouldmatchers" % "3.2.19" % Test,
  "org.wiremock"   % "wiremock"                 % "3.13.1" % Test
)

// Spark 4.1.x Jackson line
val jacksonCoreV = "2.20.0"
val jacksonAnnV  = "2.20"

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core"   % "jackson-core"          % jacksonCoreV,
  "com.fasterxml.jackson.core"   % "jackson-databind"      % jacksonCoreV,
  "com.fasterxml.jackson.core"   % "jackson-annotations"   % jacksonAnnV,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonCoreV
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "4.1.1" % Provided,
  "org.slf4j"        %  "slf4j-api" % "2.0.17" % Provided,

  // Do not bundle Jackson in published jar; Spark provides it.
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonCoreV % Provided,

  "org.influxdb"     %  "influxdb-java" % "2.25",

  // Use Kafka 3.x line to reduce conflicts with Spark environments (avoid Kafka 4.x)
  "org.apache.kafka" %  "kafka-clients" % "3.9.1"
) ++ testDeps

// ─── Test JVM flags (needed by TaskMetricsTest & StageMetricsTest) ─────────────
Test / fork := true
Test / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)

// ─── Publishing ────────────────────────────────────────────────────────────────
Compile / packageSrc / publishArtifact := true
Compile / packageDoc / publishArtifact := true

ThisBuild / publishTo := {
  val snapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at snapshots)
  else localStaging.value
}
ThisBuild / organizationHomepage := Some(url("https://github.com/LucaCanali"))
ThisBuild / versionScheme := Some("early-semver")

// ─── Project metadata ─────────────────────────────────────────────────────────
organization := "ch.cern.sparkmeasure"
description  := "sparkMeasure is a tool for performance troubleshooting of Apache Spark workloads."

developers := List(
  Developer(
    id    = "LucaCanali",
    name  = "Luca Canali",
    email = "Luca.Canali@cern.ch",
    url("https://github.com/LucaCanali")
  )
)

homepage := Some(url("https://github.com/LucaCanali/sparkMeasure"))

scmInfo := Some(
  ScmInfo(
    browseUrl  = url("https://github.com/LucaCanali/sparkMeasure"),
    connection = "scm:git:git@github.com:LucaCanali/sparkMeasure.git"
  )
)

