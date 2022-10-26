name := "spark-measure"

version := "0.23-SNAPSHOT"

scalaVersion := "2.12.15"
crossScalaVersions := Seq("2.12.15", "2.13.8")

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

// publishing to Sonatype Nexus repository and Maven
publishMavenStyle := true
isSnapshot := true

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.3"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.36"
libraryDependencies += "org.influxdb" % "influxdb-java" % "2.14"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.2.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.9" % "test"
libraryDependencies += "com.github.tomakehurst" % "wiremock" % "2.27.2" % "test"

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
