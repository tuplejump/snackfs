import sbt._
import sbt.Keys._

object SnackfsBuild extends Build {

  lazy val snackfs = Project(
    id = "snackfs",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "snackfs",
      organization := "tj",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.9.3",

      libraryDependencies ++= Seq("org.apache.hadoop" % "hadoop-common" % "0.22.0",
        "org.apache.cassandra" % "cassandra-thrift" % "1.2.6",
        "org.apache.cassandra" % "cassandra-all" % "1.2.6",
        "org.scalatest" %% "scalatest" % "1.9.1" % "test",
        "org.apache.commons" % "commons-io" % "1.3.2"
      )
    )
  )
}
