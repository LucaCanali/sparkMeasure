name := "spark-measure"

version := "0.25-SNAPSHOT"

scalaVersion := "2.12.18"
crossScalaVersions := Seq("2.12.18", "2.13.8")

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

// publishing to Sonatype Nexus repository and Maven
publishMavenStyle := true
isSnapshot := true

val testDeps = Seq(
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "org.scalatest" %% "scalatest-shouldmatchers" % "3.2.19" % Test,
  "org.wiremock" % "wiremock" % "3.13.0" % Test
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.5",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.18.3",
  "org.slf4j" % "slf4j-api" % "2.0.17",
  "org.influxdb" % "influxdb-java" % "2.25",
  "org.apache.kafka" % "kafka-clients" % "3.9.0"
) ++ testDeps

// This is for kafka-clients conflicting with Spark 3.5.5 jni dependency, remove with Spark 4.x
dependencyOverrides += "com.github.luben" % "zstd-jni" % "1.5.5-4"

// This is for tests, TaskMetricsTest and StageMetricsTest need to access these java packages
Test / fork := true        // separate JVM so the flags are used
Test / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)

organization := "ch.cern.sparkmeasure"
description := "sparkMeasure is a tool for performance troubleshooting of Apache Spark workloads."
developers := List(Developer(
  "LucaCanali", "Luca Canali", "Luca.Canali@cern.ch",
  url("https://github.com/LucaCanali")
))
homepage := Some(url("https://github.com/LucaCanali/sparkMeasure"))

publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

scmInfo := Some(
  ScmInfo(
    url("https://github.com/LucaCanali/sparkMeasure"),
    "scm:git@github.com:LucaCanali/sparkMeasure.git"
  )
)
