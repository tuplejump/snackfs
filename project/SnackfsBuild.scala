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

  lazy val cas_version = "1.2.12"

  lazy val dist = TaskKey[Unit]("dist", "Generates project distribution")

  lazy val snackfs = Project(
    id = "snackfs",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "snackfs",
      organization := "com.tuplejump",
      version := "0.5-EA",
      scalaVersion := "2.9.3",
      parallelExecution in Test := false,
      retrieveManaged := true,

      libraryDependencies ++= Seq("org.apache.hadoop" % "hadoop-core" % "1.0.4",
        "org.apache.cassandra" % "cassandra-thrift" % cas_version,
        "org.apache.cassandra" % "cassandra-all" % cas_version,
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

      val destination = "target/SnackFS-%s/".format(v)
      val lib = destination + "lib/"
      val bin = destination + "bin/"
      val conf = destination + "conf/"

      IO.copyFile(f, new File(lib + f.getName))

      /*Dependencies*/
      IO.copyFile(new File(ivyHome + "org.scala-lang/scala-library/jars/scala-library-2.9.3.jar"),
        new File(lib + "scala-library-2.9.3.jar"))

      val jars = getLibraries
      jars.foreach(j => {
        val jarFile = new File(j)
        IO.copyFile(jarFile, new File(lib + jarFile.getName))
      })

      /*script and configuration */
      val shellBin: sbt.File = new File(bin + "snackfs")
      IO.copyFile(new File("src/main/scripts/snackfs"), shellBin)
      shellBin.setExecutable(true, false)
      IO.copyFile(new File("src/main/resources/core-site.xml"), new File(conf + "core-site.xml"))

      val jarFiles = IO.listFiles(new File(lib))
      val configFiles = IO.listFiles(new File(conf))
      val scriptFiles = IO.listFiles(new File(bin))
      val allFiles = jarFiles ++ configFiles ++ scriptFiles
      val fileSeq = for (f <- allFiles) yield (f, f.getPath)

      val distTgz: sbt.File = new File("target/snackfs-%s.tgz".format(v))
      val tarball: sbt.File = makeTarball("snackfs-%s".format(v), new File(destination), new File("target"))
      IO.gzip(tarball, distTgz)

      IO.delete(tarball)
      IO.delete(new File(destination))
      s.log.info("SnackFS Distribution created at %s".format(distTgz.getAbsolutePath))
  }

  def getLibraries: List[String] = {
    val jarSource = "lib_managed/jars/"

    val cassandra = jarSource + "org.apache.cassandra/"
    val cassandraRelated = List(cassandra + "cassandra-all/cassandra-all-" + cas_version + ".jar",
      cassandra + "cassandra-thrift/cassandra-thrift-" + cas_version + ".jar",
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

    val logger = jarSource + "com.twitter/"
    val loggingRelated = List(logger + "util-app_2.9.2/util-app_2.9.2-6.7.0.jar",
      logger + "util-core_2.9.2/util-core_2.9.2-6.7.0.jar",
      logger + "util-logging_2.9.2/util-logging_2.9.2-6.7.0.jar")

    val requiredJars = cassandraRelated ++ hadoopRelated ++ otherHadoopDeps ++ loggingRelated
    requiredJars
  }

  def makeTarball(name: String, tarDir: File, rdir: File /* mappings: Seq[(File, String)]*/): File = {
    val tarball = new File("target") / (name + ".tar")
    val process: ProcessBuilder = Process(Seq("tar", "-pcf", tarball.getAbsolutePath, tarDir.getName), Some(rdir))
    process.! match {
      case 0 => ()
      case n => sys.error("Error tarballing " + tarball + ". Exit code: " + n)
    }
    tarball
  }

}
