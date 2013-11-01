/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Tuplejump Software Pvt. Ltd. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import sbt._
import sbt.Keys._

object SnackfsBuild extends Build {

  lazy val dist = TaskKey[Unit]("dist", "Generates project distribution")

  lazy val snackfs = Project(
    id = "snackfs",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "snackfs",
      organization := "tj",
      version := "0.3-SNAPSHOT",
      scalaVersion := "2.9.3",
      parallelExecution in Test := false,
      retrieveManaged := true,

      libraryDependencies ++= Seq("org.apache.hadoop" % "hadoop-core" % "1.0.4",
        "org.apache.cassandra" % "cassandra-thrift" % "1.2.9",
        "org.apache.cassandra" % "cassandra-all" % "1.2.9",
        "commons-pool" % "commons-pool" % "1.6",
        "org.scalatest" %% "scalatest" % "1.9.1" % "test",
        "org.apache.commons" % "commons-io" % "1.3.2" % "test",
        "com.novocode" % "junit-interface" % "0.10" % "test",
        "org.apache.commons" % "commons-lang3" % "3.1" % "test",
        "com.twitter" % "util-logging_2.9.2" % "6.7.0"
      )
    ) ++ Seq(distTask)
  )

  def distTask = dist in Compile <<= (packageBin in Compile, version in Compile, streams) map {
    (f: File, v: String, s) =>
      val userHome = System.getProperty("user.home")
      val ivyHome = userHome + "/.ivy2/cache/" //should be updated to point to ivy cache if its not in home directory

      val destination = "SnackFS/"
      val lib = destination + "lib/"
      val bin = destination + "bin/"
      val conf = destination + "conf/"

      IO.copyFile(f, new File("SnackFS/lib/" + f.getName))

      /*Dependencies*/
      IO.copyFile(new File(ivyHome + "org.scala-lang/scala-library/jars/scala-library-2.9.3.jar"),
        new File(lib + "scala-library-2.9.3.jar"))

      val jars = getLibraries
      jars.foreach(j => {
        val jarFile = new File(j)
        IO.copyFile(jarFile, new File(lib + jarFile.getName))
      })

      /*script and configuration */
      IO.copyFile(new File("src/main/scripts/snackfs"), new File(bin + "snackfs"))
      IO.copyFile(new File("src/main/resources/core-site.xml"), new File(conf + "core-site.xml"))

      val jarFiles = IO.listFiles(new File(lib))
      val configFiles = IO.listFiles(new File(conf))
      val scriptFiles = IO.listFiles(new File(bin))
      val allFiles = jarFiles ++ configFiles ++ scriptFiles
      val fileSeq = for (f <- allFiles) yield (f, f.getPath)

      val distZip: sbt.File = new File("target/snackfs-%s.tar.gz".format(v))
      IO.zip(fileSeq, distZip)
      IO.delete(new File(destination))
      s.log.info("SnackFS Distribution created at %s".format(distZip.getAbsolutePath))
  }

  def getLibraries: List[String] = {
    val jarSource = "lib_managed/jars/"

    val cassandra = jarSource + "org.apache.cassandra/"
    val cassandraRelated = List(cassandra + "cassandra-all/cassandra-all-1.2.9.jar",
      cassandra + "cassandra-thrift/cassandra-thrift-1.2.9.jar",
      jarSource + "org.apache.thrift/libthrift/libthrift-0.7.0.jar",
      jarSource + "commons-pool/commons-pool/commons-pool-1.6.jar"
    )

    val hadoopRelated = List(jarSource + "org.apache.hadoop/hadoop-core/hadoop-core-1.0.4.jar",
      jarSource + "commons-cli/commons-cli/commons-cli-1.2.jar",
      jarSource + "commons-configuration/commons-configuration/commons-configuration-1.6.jar",
      jarSource + "commons-lang/commons-lang/commons-lang-2.6.jar",
      jarSource + "commons-logging/commons-logging/commons-logging-1.1.1.jar"
    )

    val jackson = jarSource + "org.codehaus.jackson/"
    val log4j = "lib_managed/bundles/log4j/log4j/"

    val otherHadoopDeps = List(jackson + "jackson-core-asl/jackson-core-asl-1.9.2.jar",
      jackson + "jackson-mapper-asl/jackson-mapper-asl-1.9.2.jar",
      log4j + "log4j-1.2.16.jar",
      jarSource + "org.slf4j/slf4j-log4j12/slf4j-log4j12-1.7.2.jar",
      jarSource + "org.slf4j/slf4j-api/slf4j-api-1.7.2.jar"
    )

    val requiredJars = cassandraRelated ++ hadoopRelated ++ otherHadoopDeps
    requiredJars
  }

}
