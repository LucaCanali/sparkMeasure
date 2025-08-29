/****************************************
 *  sparkMeasure – build definition     *
 ****************************************/

name := "spark-measure"

version := "0.26"
isSnapshot := false

scalaVersion       := "2.12.18"
crossScalaVersions := Seq("2.12.18", "2.13.8")

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

// ─── Dependencies ──────────────────────────────────────────────────────────────
val testDeps = Seq(
  "org.scalatest"      %% "scalatest"               % "3.2.19" % Test,
  "org.scalatest"      %% "scalatest-shouldmatchers"% "3.2.19" % Test,
  "org.wiremock"        % "wiremock"                % "3.13.0" % Test
)

libraryDependencies ++= Seq(
  "org.apache.spark"            %% "spark-sql"            % "3.5.6",
  "com.fasterxml.jackson.module"%% "jackson-module-scala" % "2.18.3",
  "org.slf4j"                    % "slf4j-api"            % "2.0.17",
  "org.influxdb"                 % "influxdb-java"        % "2.25",
  "org.apache.kafka"             % "kafka-clients"        % "3.9.1"
) ++ testDeps

// ── Override to resolve Kafka/Spark JNI clash (remove once on Spark 4.x) ──────
dependencyOverrides += "com.github.luben" % "zstd-jni" % "1.5.5-4"

// ─── Test JVM flags (needed by TaskMetricsTest & StageMetricsTest) ─────────────
Test / fork := true
Test / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)

// ─── Publishing ────────────────────────────────────────────────────────────────
publishMavenStyle := true

publishTo := Some {
  if (isSnapshot.value)
    Opts.resolver.sonatypeOssSnapshots.head
  else
    Opts.resolver.sonatypeStaging
}

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
    connection = "scm:git@github.com:LucaCanali/sparkMeasure.git"
  )
)

