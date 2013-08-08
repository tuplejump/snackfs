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
      // add other settings here

      libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "0.22.0"

  )
  )
}
