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
package com.tuplejump.fs

import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.scalatest.matchers.MustMatchers
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import java.io.{FileNotFoundException, IOException}

class SnackFSSpec extends FlatSpec with BeforeAndAfterAll with MustMatchers {

  val isTrue = true
  val isFalse = false

  val fs = SnackFS()
  val uri = URI.create("snackfs://localhost:9000")
  fs.initialize(uri, new Configuration())

  it should "create a new filesystem with given store" in {
    fs.getUri must be(uri)
    val user = System.getProperty("user.name", "none")
    fs.getWorkingDirectory must be(new Path("snackfs://localhost:9000/user/" + user))
  }

  it should "add a directory" in {
    val result = fs.mkdirs(new Path("/mytestdir"))
    assert(result === isTrue)
  }

  it should "create an entry for a file" in {
    val fsData = fs.create(new Path("/home/Downloads/JSONParser.js"))
    fsData.write("SOME CONTENT".getBytes)
    val position = fsData.getPos
    position must be(12)
  }

  it should "not when trying to add an existing file as a directory" in {
    val fsData = fs.create(new Path("/home/Downloads/someTest"))
    fsData.write("SOME CONTENT".getBytes)
    fsData.close()
    val path = new Path("/home/Downloads/someTest")
    fs.mkdirs(path) must be(isFalse)
  }

  it should "allow to read from a file" in {
    val fsData = fs.create(new Path("/home/Downloads/random"))
    fsData.write("SOME CONTENT".getBytes)
    fsData.close()

    val is = fs.open(new Path("/home/Downloads/random"))
    var dataArray = new Array[Byte](12)
    is.readFully(0, dataArray)
    is.close()

    val result = new String(dataArray)
    result must be("SOME CONTENT")
  }

  it should "throw an exception when trying to open a directory" in {
    val path = new Path("/test")
    fs.mkdirs(path)
    val exception = intercept[IOException] {
      fs.open(path)
    }
    exception.getMessage must be("Path %s is a directory.".format(path))
  }

  it should "throw an exception when trying to open a file which doesn't exist" in {
    val path = new Path("/newFile")
    val exception = intercept[IOException] {
      fs.open(path)
    }
    exception.getMessage must be("No such file.")
  }

  it should "get file status" in {
    val path = new Path("/home/Downloads/testStatus")
    val fsData = fs.create(path)
    fsData.write("SOME CONTENT".getBytes)
    fsData.close()

    val status = fs.getFileStatus(path)
    !status.isDir must be(isTrue)
    status.getLen must be(12)
    status.getPath must be(path)
  }

  it should "get file block locations" in {
    val path = new Path("/home/Downloads/testLocations")
    val fsData = fs.create(path)
    fsData.write("This is a test to check the block location details".getBytes)
    fsData.write("This is a test to check the block location details".getBytes)
    fsData.write("This is a test to check the block location details".getBytes)
    fsData.write("This is a test to check the block location details".getBytes)
    fsData.write("This is a test to check the block location details".getBytes)

    fsData.close()

    val status = fs.getFileStatus(path)
    val locations = fs.getFileBlockLocations(status, 0, 10)
    assert(locations(0).getLength === 250)
  }

  it should "list all files/directories within the given directory" in {
    val dirPath1 = new Path("/tmp/user")
    fs.mkdirs(dirPath1)
    val dirPath2 = new Path("/tmp/local")
    fs.mkdirs(dirPath2)

    val filePath1 = new Path("/tmp/testFile")
    val fileData1 = fs.create(filePath1)
    fileData1.write("This is a test to check list functionality".getBytes)
    fileData1.close()

    val filePath2 = new Path("/tmp/user/file")
    val fileData2 = fs.create(filePath2)
    fileData2.write("This is a test to check list functionality".getBytes)
    fileData2.close()

    val baseDirPath = new Path("/tmp")
    val result = fs.listStatus(baseDirPath)
    result.length must be(3)
    result.filter(!_.isDir).length must be(1)
    result.filter(_.isDir).length must be(2)
  }

  it should "delete all files/directories within the given directory" in {
    val dirPath1 = new Path("/tmp1/user1")
    fs.mkdirs(dirPath1)
    val dirPath2 = new Path("/tmp1/local1")
    fs.mkdirs(dirPath2)

    val filePath1 = new Path("/tmp1/testFile1")
    val fileData1 = fs.create(filePath1)
    fileData1.write("This is a test to check delete functionality".getBytes)
    fileData1.close()

    val filePath2 = new Path("/tmp1/user1/file")
    val fileData2 = fs.create(filePath2)
    fileData2.write("This is a test to check delete functionality".getBytes)
    fileData2.close()

    val dirStatus = fs.getFileStatus(dirPath2)
    dirStatus.isDir must be(isTrue)

    val baseDirPath = new Path("/tmp1")
    val result = fs.delete(baseDirPath, isTrue)
    result must be(isTrue)

    val exception1 = intercept[FileNotFoundException] {
      fs.getFileStatus(dirPath2)
    }
    exception1.getMessage must be("No such file exists")

    val exception2 = intercept[FileNotFoundException] {
      fs.getFileStatus(filePath2)
    }
    exception2.getMessage must be("No such file exists")

    val exception3 = intercept[FileNotFoundException] {
      fs.getFileStatus(baseDirPath)
    }
    exception3.getMessage must be("No such file exists")

  }

  it should "rename a file" in {

    val filePath1 = new Path("/tmp2/testRename")
    val fileData1 = fs.create(filePath1)
    fileData1.write("This is a test to check rename functionality".getBytes)
    fileData1.close()

    val filePath2 = new Path("/tmp2/newName")

    val result = fs.rename(filePath1, filePath2)

    result must be(isTrue)

    val exception2 = intercept[FileNotFoundException] {
      fs.getFileStatus(filePath1)
    }
    exception2.getMessage must be("No such file exists")

    val fileStatus = fs.getFileStatus(filePath2)
    !fileStatus.isDir must be(isTrue)
  }

  it should "rename a directory" in {

    val dirPath1 = new Path("/abc/user")
    fs.mkdirs(dirPath1)
    val dirPath2 = new Path("/abc/local")
    fs.mkdirs(dirPath2)

    val filePath1 = new Path("/abc/testfile")
    val fileData1 = fs.create(filePath1)
    fileData1.write("This is a test to check rename functionality".getBytes)
    fileData1.close()

    val filePath2 = new Path("/abc/jkl/testfile")
    val fileData2 = fs.create(filePath2)
    fileData2.write("This is a test to check rename functionality".getBytes)
    fileData2.close()

    val baseDirPath = new Path("/abc")
    val dirStatus1 = fs.listStatus(new Path("/abc"))
    dirStatus1.filter(!_.isDir).length must be(1)

    fs.mkdirs(new Path("/pqr"))
    fs.rename(baseDirPath, new Path("/pqr/lmn"))

    val dirStatus = fs.listStatus(new Path("/pqr/lmn"))
    dirStatus.filter(!_.isDir).length must be(1)
    dirStatus.filter(_.isDir).length must be(3)

    val fileStatus2 = fs.getFileStatus(new Path("/pqr/lmn/jkl/testfile"))
    !fileStatus2.isDir must be(isTrue)
  }
}
