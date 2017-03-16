name := "spark-measure"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.8"
    
resolvers += Resolver.mavenLocal
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

isSnapshot := true

spIgnoreProvided := true
sparkVersion := "2.1.0"
sparkComponents := Seq("sql")

libraryDependencies += "org.apache.logging.log4j" % "log4j" % "2.8"
