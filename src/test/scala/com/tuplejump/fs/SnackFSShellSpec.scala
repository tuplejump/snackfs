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

/**
 * To run the tests set hadoopHome in executeAndGetOutput() and projectHome
 */
package com.tuplejump.fs

import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.scalatest.matchers.MustMatchers
import java.io.{File, IOException}
import org.apache.commons.io.IOUtils
import scala.io.Source
import java.util.Date

class SnackFSShellSpec extends FlatSpec with BeforeAndAfterAll with MustMatchers {

  val hadhome = System.getenv("HADOOP_HOME")

  assert(hadhome != null && !hadhome.isEmpty, "Must have hadoop, and must set HADOOP_HOME before running these tests")


  def executeAndGetOutput(command: Seq[String]): String = {

    val hadoopHome = new File(hadhome + File.separator + "bin")

    val builder = new ProcessBuilder(command: _*)
      .directory(hadoopHome)

    val process = builder.start()
    val error = new String(IOUtils.toByteArray(process.getErrorStream))
    val output = IOUtils.readLines(process.getInputStream)
    if (error.length > 0) {
      throw new IOException(error)
    }
    output.toString
  }

  val isTrue = true

  val hadoopFSCommands = Seq("./hadoop", "fs")

  val timestamp = new Date().getTime
  val basePath = "testFSShell" + timestamp

  val base = "snackfs://localhost:9000/test" + timestamp + "/"

  val testingDir = base + "testFSShell/"
  val projectHome = "/snackfs/src/" //to be set

  val listCommand = hadoopFSCommands ++ Seq("-lsr", base)

  //mkdir
  it should "make a new directory" in {
    val command = hadoopFSCommands ++ Seq("-mkdir", testingDir)
    executeAndGetOutput(command)
    val listoutPut = executeAndGetOutput(listCommand)
    listoutPut must include("/testFSShell")
  }

  it should "not make a new directory with the name of an existing one" in {
    val command = hadoopFSCommands ++ Seq("-mkdir", testingDir)
    val exception = intercept[IOException] {
      executeAndGetOutput(command)
    }
    val errMsg = "mkdir: cannot create directory"
    exception.getMessage must startWith(errMsg)
  }

  //copyFromLocal
  it should "copy a file into the filesystem using copyFromLocal" in {
    val source = projectHome + "test/resources/small.txt"
    val command = hadoopFSCommands ++ Seq("-copyFromLocal", source, testingDir)
    executeAndGetOutput(command)

    val listoutPut = executeAndGetOutput(listCommand)
    listoutPut must include("small.txt")
  }

  it should "not overwrite a file into the filesystem using copyFromLocal" in {
    val source = projectHome + "test/resources/small.txt"
    val command = hadoopFSCommands ++ Seq("-copyFromLocal", source, testingDir)
    val exception = intercept[IOException] {
      executeAndGetOutput(command)
    }
    exception.getMessage must include("already exists")
  }

  //copyToLocal
  it should "copy a file from the filesystem using copyToLocal" in {
    val destination = projectHome + "test/resources/" + basePath + "/TestSmall.txt"
    val source = testingDir + "small.txt"
    val command = hadoopFSCommands ++ Seq("-copyToLocal", source, destination)
    executeAndGetOutput(command)

    val copiedFile = new File(destination)
    copiedFile.exists() must be(isTrue)
  }

  it should "not overwrite a file using copyToLocal" in {
    val destination = projectHome + "test/resources/small.txt"
    val source = testingDir + "small.txt"
    val command = hadoopFSCommands ++ Seq("-copyToLocal", source, destination)
    val exception = intercept[IOException] {
      executeAndGetOutput(command)
    }
    exception.getMessage must include("already exists")
  }

  //get
  it should "copy a file from the filesystem using get" in {
    val destination = projectHome + "test/resources/" + basePath + "/TestGetSmall.txt"
    val source = testingDir + "small.txt"
    val command = hadoopFSCommands ++ Seq("-copyToLocal", source, destination)
    executeAndGetOutput(command)

    val copiedFile = new File(destination)
    copiedFile.exists() must be(isTrue)
  }

  //cat
  it should "print file content" in {
    val source = projectHome + "test/resources/vsmall.txt"
    val writeCommand = hadoopFSCommands ++ Seq("-copyFromLocal", source, testingDir)
    executeAndGetOutput(writeCommand)
    val readCommand = hadoopFSCommands ++ Seq("-cat", testingDir + "/vsmall.txt")
    val output = executeAndGetOutput(readCommand)
    val fileContent = IOUtils.readLines(Source.fromFile(new File(source)).bufferedReader()).toString
    output must be(fileContent)
  }

  //cp
  it should "copy all files from a directory into another" in {
    val destName = "testCpCommand"
    val destination = base + destName + "/"
    val source = testingDir
    val command = hadoopFSCommands ++ Seq("-cp", source, destination)
    executeAndGetOutput(command)
    val listoutPut = executeAndGetOutput(listCommand)
    listoutPut must include("/" + destName + "/small.txt")
    listoutPut must include("/" + destName + "/vsmall.txt")
  }

  //du
  it should "display aggregate length of files in a directory" in {
    val command = hadoopFSCommands ++ Seq("-du", base)
    val output = executeAndGetOutput(command)
    output must include(base + "testFSShell")
    output must include(base + "testCpCommand")
    output must startWith("[Found 2 items, 598419")
  }

  it should "display aggregate length of file" in {
    val command = hadoopFSCommands ++ Seq("-du", testingDir + "vsmall.txt")
    val output = executeAndGetOutput(command)
    output must startWith("[Found 1 items, 623 ")
    output must endWith("/testFSShell/vsmall.txt]")
  }

  //dus
  it should "display summary of file lengths" in {
    val command = hadoopFSCommands ++ Seq("-dus", base)
    val output = executeAndGetOutput(command)
    output must include("/test" + timestamp)
    output must include("1196838")
  }

  //ls
  it should "list children of directory" in {
    val command = hadoopFSCommands ++ Seq("-ls", base)
    val output = executeAndGetOutput(command)
    output must startWith("[Found 2 items,")
    output must include("/testFSShell")
    output must include("/testCpCommand")
  }

  it should "list stats of a file" in {
    val command = hadoopFSCommands ++ Seq("-ls", testingDir + "vsmall.txt")
    val output = executeAndGetOutput(command)
    output must startWith("[Found 1 items,")
    output must include("/testFSShell/vsmall.txt")
  }

  //lsr
  it should "list children of directory recursive" in {
    val output = executeAndGetOutput(listCommand)
    output must include("/testFSShell")
    output must include("/testFSShell/small.txt")
    output must include("/testFSShell/vsmall.txt")
    output must include("/testCpCommand")
    output must include("/testCpCommand/small.txt")
    output must include("/testCpCommand/vsmall.txt")
  }

  /*//moveFromLocal -- docs and behaviour are contradicting
  it should "result in not implemented" in {
    val source = projectHome + "test/resources/small.txt"
    val command = hadoopFSCommands ++ Seq("-moveFromLocal", source, testingDir)
    val output = executeAndGetOutput(command)
    println(output)
  }*/

  it should "move a file" in {
    val source = testingDir + "small.txt"
    val destination = base + "small.txt"
    val command = hadoopFSCommands ++ Seq("-mv", source, destination)
    executeAndGetOutput(command)
    val output = executeAndGetOutput(listCommand)
    output must include("/testFSShell")
    output must not include "/testFSShell/small.txt"
    output must include("/small.txt")
  }

  //put (reading from stdin also works)
  it should "copy a file into the filesystem using put" in {
    val source = projectHome + "test/resources/vsmall.txt"
    val command = hadoopFSCommands ++ Seq("-put", source, base)
    executeAndGetOutput(command)
    val listOutPut = executeAndGetOutput(listCommand)
    listOutPut must include("/vsmall.txt")
  }

  it should "copy multiple files into the filesystem using put" in {
    val source1 = projectHome + "test/resources/small.txt"
    val source2 = projectHome + "test/resources/vsmall.txt"
    val destination = base + "testPutCommand/"
    val mkdirCommand = hadoopFSCommands ++ Seq("-mkdir", destination)
    executeAndGetOutput(mkdirCommand)
    val command = hadoopFSCommands ++ Seq("-put", source1, source2, destination)
    executeAndGetOutput(command)
    val listOutPut = executeAndGetOutput(listCommand)
    listOutPut must include("/testPutCommand/vsmall.txt")
    listOutPut must include("/testPutCommand/small.txt")
  }

  //stat
  it should "display stat" in {
    val command = hadoopFSCommands ++ Seq("-stat", base)
    val output = executeAndGetOutput(command)
    output must not be "[]"
  }

  //tail
  it should "display last KB of a file" in {
    val readCommand = hadoopFSCommands ++ Seq("-tail", base + "/vsmall.txt")
    val output = executeAndGetOutput(readCommand)
    output.length must not be 0
  }

  //touchz
  it should "create a file of zero length" in {
    val command = hadoopFSCommands ++ Seq("-touchz", base + "emptyFile.txt")
    executeAndGetOutput(command)
    val listOutPut = executeAndGetOutput(listCommand)
    listOutPut must include("/emptyFile.txt")
  }

  it should "move multiple files" in {
    val source1 = base + "small.txt"
    val source2 = base + "vsmall.txt"
    val destination = base + "testMvCommand/"
    val mkdirCommand = hadoopFSCommands ++ Seq("-mkdir", destination)
    executeAndGetOutput(mkdirCommand)
    val command = hadoopFSCommands ++ Seq("-mv", source1, source2, destination)
    executeAndGetOutput(command)
    val listOutPut = executeAndGetOutput(listCommand)
    listOutPut must include("/testMvCommand/small.txt")
    listOutPut must include("/testMvCommand/vsmall.txt")
  }

  //rm
  it should "remove a file" in {
    val command = hadoopFSCommands ++ Seq("-rm", testingDir + "vsmall.txt")
    val output = executeAndGetOutput(command)
    output must startWith("[Deleted")
    output must include("/testFSShell/vsmall.txt")
  }

  //rmr
  it should "remove a directory and all its contents" in {
    val command = hadoopFSCommands ++ Seq("-rmr", base + "testPutCommand/")
    val output = executeAndGetOutput(command)
    output must startWith("[Deleted")
    output must include("/testPutCommand")
  }

  override def afterAll() = {
    //remove files generated in resources
    val rmdirCommand = hadoopFSCommands ++ Seq("-rmr", projectHome + "test/resources/" + basePath)
    executeAndGetOutput(rmdirCommand)

    //remove the test directory
    val rmTestCommand = hadoopFSCommands ++ Seq("-rmr", base)
    executeAndGetOutput(rmTestCommand)
  }

  override def beforeAll() = {
    //make directory in resources for test
    val mkdirCommand = hadoopFSCommands ++ Seq("-mkdir", projectHome + "test/resources/" + basePath)
    executeAndGetOutput(mkdirCommand)
  }
}
