/****************************************
 *  sparkMeasure – build definition     *
 ****************************************/

name := "spark-measure"

version := "0.27-SNAPSHOT"

scalaVersion       := "2.12.18"
crossScalaVersions := Seq("2.12.18", "2.13.16")

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

// ─── Dependencies ──────────────────────────────────────────────────────────────
val testDeps = Seq(
  "org.scalatest"      %% "scalatest"               % "3.2.19" % Test,
  "org.scalatest"      %% "scalatest-shouldmatchers"% "3.2.19" % Test,
  "org.wiremock"        % "wiremock"                % "3.13.1" % Test
)

libraryDependencies ++= Seq(
  "org.apache.spark"            %% "spark-sql"            % "3.5.7",
  "com.fasterxml.jackson.module"%% "jackson-module-scala" % "2.20.0",
  "org.slf4j"                    % "slf4j-api"            % "2.0.17",
  "org.influxdb"                 % "influxdb-java"        % "2.25",
  "org.apache.kafka"             % "kafka-clients"        % "4.1.0"
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

