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
import java.net.URI
import org.apache.hadoop.fs.Path
import org.apache.cassandra.utils.ByteBufferUtil
import scala.concurrent.Await
import java.nio.file.{FileSystems, Files}
import org.apache.commons.io.IOUtils
import org.scalatest.matchers.MustMatchers
import org.apache.cassandra.locator.SimpleStrategy
import com.tuplejump.model.SnackFSConfiguration
import org.apache.hadoop.conf.Configuration

class FileSystemStreamSpec extends FlatSpec with BeforeAndAfterAll with MustMatchers {
  val configuration = new Configuration()
  configuration.set("fs.keyspace", "STREAM")
  val snackFSConfiguration = SnackFSConfiguration.get(configuration)

  val store = new ThriftStore(snackFSConfiguration)
  store.init

  val replicationStrategy = classOf[SimpleStrategy].getCanonicalName
  Await.result(store.createKeyspace, snackFSConfiguration.atMost)
  //Await.result(store.init, snackFSConfiguration.atMost)

  it should "fetch data which is equal to actual data" in {
    val pathURI = URI.create("outputStream.txt")
    val path = new Path(pathURI)
    val dataString: String = "Test Subblock insertion"
    val data = ByteBufferUtil.bytes(dataString)

    val outputStream = FileSystemOutputStream(store, path, 30, 10, 10,snackFSConfiguration.atMost)

    outputStream.write(data.array(), 0, data.array().length)
    outputStream.close()

    val inode = Await.result(store.retrieveINode(path), snackFSConfiguration.atMost)
    assert(inode.blocks.length === 1)

    val blockData = store.retrieveBlock(inode.blocks(0))
    var outBuf: Array[Byte] = new Array[Byte](23)
    blockData.read(outBuf, 0, 23)
    assert(outBuf != null)
    new String(outBuf) must be(dataString)
  }

  it should "fetch data loaded from smaller(<2KB) file" in {
    val nioPath = FileSystems.getDefault.getPath("src/test/resources/vsmall.txt")
    val data = Files.readAllBytes(nioPath)

    //println("file size=" + data.length)
    val pathURI = URI.create("vsmall.txt")
    val path = new Path(pathURI)
    val maxBlockSize = 500
    val maxSubBlockSize = 50
    val outputStream = FileSystemOutputStream(store, path, maxBlockSize, maxSubBlockSize, data.length,snackFSConfiguration.atMost)
    outputStream.write(data, 0, data.length)
    outputStream.close()

    val inode = Await.result(store.retrieveINode(path), snackFSConfiguration.atMost)
    ////println("blocks=" + inode.blocks.length)
    val minSize: Int = data.length / maxBlockSize
    ////println(minSize)
    assert(inode.blocks.length >= minSize)
    var fetchedData: Array[Byte] = new Array[Byte](data.length)
    var offset = 0
    inode.blocks.foreach(block => {
      val blockData = store.retrieveBlock(block)
      val source = IOUtils.toByteArray(blockData)
      System.arraycopy(source, 0, fetchedData, offset, source.length)
      blockData.close()
      offset += block.length.asInstanceOf[Int]
    })
    //println("completed copy")
    new String(fetchedData) must be(new String(data))
  }

  it should "fetch data loaded from medium(~600KB) file" in {
    val nioPath = FileSystems.getDefault.getPath("src/test/resources/small.txt")
    val data = Files.readAllBytes(nioPath)

    val dataString = new java.lang.String(data)

    //println("file size=" + data.length)
    val pathURI = URI.create("small.txt")
    val path = new Path(pathURI)
    val maxBlockSize: Int = 30000
    val maxSubBlockSize = 3000
    val outputStream = FileSystemOutputStream(store, path, maxBlockSize, maxSubBlockSize, data.length,snackFSConfiguration.atMost)
    outputStream.write(data, 0, data.length)
    outputStream.close()

    val inode = Await.result(store.retrieveINode(path), snackFSConfiguration.atMost)
    //println("blocks=" + inode.blocks.length)
    val minSize: Int = data.length / maxBlockSize
    //println(minSize)
    assert(inode.blocks.length >= minSize)

    var fetchedData: Array[Byte] = Array[Byte]()
    var offset = 0
    inode.blocks.foreach(block => {
      val blockData = store.retrieveBlock(block)
      val source = IOUtils.toByteArray(blockData)
      blockData.close()
      fetchedData = fetchedData ++ source
      offset += source.length
    })
    //println("completed copy")
    val fetchedDataString = new String(fetchedData)
    fetchedData.length must be(data.length)
    fetchedDataString must be(dataString)
  }

  it should "result in small file (<2KB) data stored through outputstream when fetched from input stream " in {
    val nioPath = FileSystems.getDefault.getPath("src/test/resources/vsmall.txt")
    val data = Files.readAllBytes(nioPath)

    //println("file size=" + data.length)
    val pathURI = URI.create("vsmall.txt")
    val path = new Path(pathURI)

    val inode = FileSystemInputStream(store, path)
    var inodeData = new Array[Byte](data.length)
    inode.read(inodeData, 0, data.length)
    inode.close()
    //println("completed copy")
    //println(inodeData.length)
    new String(inodeData) must be(new String(data))
  }

  it should "result in medium file (~600KB)data stored through outputstream when fetched from input stream " in {
    val nioPath = FileSystems.getDefault.getPath("src/test/resources/small.txt")
    val data = Files.readAllBytes(nioPath)

    //println("file size=" + data.length)
    val pathURI = URI.create("small.txt")
    val path = new Path(pathURI)

    val inode = FileSystemInputStream(store, path)
    var inodeData = new Array[Byte](data.length)
    inode.read(inodeData, 0, data.length)
    inode.close()
    //println("completed copy")
    //println(inodeData.length)
    inodeData must be(data)
  }

  it should "result in small file (<2KB) data stored through outputstream when fetched from input stream using readFully" in {
    val nioPath = FileSystems.getDefault.getPath("src/test/resources/vsmall.txt")
    val data = Files.readAllBytes(nioPath)

    //println("file size=" + data.length)
    val pathURI = URI.create("vsmall.txt")
    val path = new Path(pathURI)

    val inode = FileSystemInputStream(store, path)
    var inodeData = new Array[Byte](data.length)
    inode.readFully(0, inodeData)
    inode.close()
    //println("completed copy")
    //println(inodeData.length)
    inodeData.length must be(data.length)
    inodeData must be(data)
  }

  it should "result in small file (<2KB) data stored through outputstream when fetched from input stream using IOUtils.toByteArray" in {
    val nioPath = FileSystems.getDefault.getPath("src/test/resources/vsmall.txt")
    val data = Files.readAllBytes(nioPath)

    //println("file size=" + data.length)
    val pathURI = URI.create("vsmall.txt")
    val path = new Path(pathURI)

    val inode = FileSystemInputStream(store, path)
    var inodeData = IOUtils.toByteArray(inode)
    inode.close()

    //println("completed copy")
    //println(inodeData.length)
    inodeData.length must be(data.length)
    inodeData must be(data)
  }

  override def afterAll() = {
    Await.ready(store.dropKeyspace, snackFSConfiguration.atMost)
    store.disconnect()
  }

}
