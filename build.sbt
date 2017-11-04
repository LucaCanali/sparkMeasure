name := "spark-measure"

version := "0.12-SNAPSHOT"

scalaVersion := "2.11.11"
    
resolvers += Resolver.mavenLocal
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

isSnapshot := true

spIgnoreProvided := true
sparkVersion := "2.1.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25"
libraryDependencies += "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.9.1" % "test"

organization := "ch.cern.sparkmeasure"

// publishing to Maven
publishMavenStyle := true

publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

homepage := Some(url("https://github.com/LucaCanali/sparkMeasure"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/LucaCanali/sparkMeasure"),
    "scm:git@github.com:LucaCanali/sparkMeasure.git"
  )
)
