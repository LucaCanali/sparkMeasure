name := "spark-measure"

version := "0.17"

scalaVersion := "2.12.10"
crossScalaVersions := Seq("2.11.12", "2.12.10")

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

isSnapshot := false

spName := "spark-measure"
sparkVersion := "2.4.6"
sparkComponents += "sql"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6"

libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.9"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.26"
libraryDependencies += "org.influxdb" % "influxdb-java" % "2.14"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.7" % "test"
libraryDependencies += "com.github.tomakehurst" % "wiremock" % "2.23.2" % "test"

// publishing to Sonatype Nexus repository and Maven
publishMavenStyle := true

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
