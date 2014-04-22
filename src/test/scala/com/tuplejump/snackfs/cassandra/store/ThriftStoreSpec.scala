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
package com.tuplejump.snackfs.cassandra.store


import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.apache.hadoop.fs.permission.FsPermission
import java.util.UUID
import java.net.URI
import org.apache.hadoop.fs.Path
import org.apache.cassandra.utils.ByteBufferUtil
import org.apache.commons.io.IOUtils
import org.scalatest.matchers.MustMatchers
import org.apache.cassandra.thrift.NotFoundException
import org.apache.hadoop.conf.Configuration
import com.tuplejump.snackfs.cassandra.model.{SnackFSConfiguration, GenericOpSuccess, Keyspace}
import com.tuplejump.snackfs.fs.model._

class ThriftStoreSpec extends FlatSpec with BeforeAndAfterAll with MustMatchers {

  val configuration = new Configuration()
  configuration.set("snackfs.keyspace", "STORE")
  val snackFSConfiguration = SnackFSConfiguration.get(configuration)
  val store = new ThriftStore(snackFSConfiguration)
  store.init

  val timestamp = System.currentTimeMillis()
  val subBlocks = List(SubBlockMeta(UUID.randomUUID, 0, 128), SubBlockMeta(UUID.randomUUID, 128, 128))
  val block1 = BlockMeta(UUID.randomUUID, 0, 256, subBlocks)
  val block2 = BlockMeta(UUID.randomUUID, 0, 256, subBlocks)
  val blocks = List(block1, block2)
  val pathURI = URI.create("testFile.txt")
  val path = new Path(pathURI)
  val iNode = INode("user", "group", FsPermission.getDefault, FileType.FILE, blocks, timestamp)

  val subBlockMeta1 = SubBlockMeta(UUID.randomUUID, 0, 128)
  val data = ByteBufferUtil.bytes("Test to store subBLock")

  it should "create a keyspace with name STORE" in {
    val ks = store.createKeyspace.get
    assert(ks.isInstanceOf[Keyspace])
  }

  /* it should "set keyspace to STORE" in {
    val result = store.init.get
    assert(result.isInstanceOf[Unit])
  }  */

  it should "create a INode" in {
    val response = store.storeINode(path, iNode)
    val responseValue: GenericOpSuccess = response.get
    assert(responseValue === GenericOpSuccess())
  }


  it should "fetch created INode" in {
    val response = store.retrieveINode(path)
    val result: INode = response.get
    assert(result === iNode)
  }

  it should "fetch created subBlock" in {
    store.storeSubBlock(block1.id, subBlockMeta1, data).get
    val storeResponse = store.retrieveSubBlock(block1.id, subBlockMeta1.id, 0)
    val response = storeResponse.get
    val responseString = new String(IOUtils.toByteArray(response))
    responseString must be(new String(data.array()))
  }

  it should "delete all the blocks of an Inode" in {
    val blockId = UUID.randomUUID
    val blockIdSecond = UUID.randomUUID

    val subBlock = SubBlockMeta(UUID.randomUUID, 0, 128)
    val subBlockSecond = SubBlockMeta(UUID.randomUUID, 0, 128)

    store.storeSubBlock(blockId, subBlock, ByteBufferUtil.bytes("Random test data")).get
    store.storeSubBlock(blockIdSecond, subBlockSecond, ByteBufferUtil.bytes("Random test data")).get

    val blockMeta = BlockMeta(blockId, 0, 0, List(subBlock))
    val blockMetaSecond = BlockMeta(blockId, 0, 0, List(subBlock))

    val subBlockData = store.retrieveSubBlock(blockMeta.id, subBlock.id, 0).get
    val dataString = new String(IOUtils.toByteArray(subBlockData))
    dataString must be("Random test data")

    val iNode = INode("user", "group", FsPermission.getDefault, FileType.FILE, List(blockMeta, blockMetaSecond), timestamp)

    store.deleteBlocks(iNode).get

    val exception = intercept[NotFoundException] {
      store.retrieveSubBlock(blockMeta.id, subBlock.id, 0).get
    }
    assert(exception.getMessage === null)
  }

  it should "fetch all sub-paths" in {
    val path1 = new Path("/tmp")
    val iNode1 = INode("user", "group", FsPermission.getDefault, FileType.DIRECTORY, null, timestamp)
    store.storeINode(path1, iNode1).get

    val path2 = new Path("/tmp/user")
    store.storeINode(path2, iNode1).get

    val path3 = new Path("/tmp/user/file")
    store.storeINode(path3, iNode).get

    val result = store.fetchSubPaths(path1, isDeepFetch = true).get
    //println(result.toString())

    result.size must be(2)
  }

  it should "fetch sub-paths" in {
    val path1 = new Path("/tmp")
    val iNode1 = INode("user", "group", FsPermission.getDefault, FileType.DIRECTORY, null, timestamp)
    store.storeINode(path1, iNode1).get

    val path2 = new Path("/tmp/user")
    store.storeINode(path2, iNode1).get

    val path3 = new Path("/tmp/user/file")
    store.storeINode(path3, iNode).get

    val result = store.fetchSubPaths(path1, isDeepFetch = false).get
    //println(result.toString())

    result.size must be(1)
  }

  it should "get block locations" in {
    val path1: Path = new Path("/tmp/user/file")

    val inode = store.retrieveINode(path1).get

    val map = store.getBlockLocations(path1).get

    map.size must be(inode.blocks.size)

  }

  /* createlock related -- locking a file so that another process cannot write to it*/
  it should "get lock when attempting for a file for the first time" in {
    val processId = UUID.randomUUID()
    val result = store.acquireFileLock(new Path("/testLock1"), processId)
    result must be(true)
  }

  it should "not get lock when attempting for a file from another process if lock is not released" in {
    val processId = UUID.randomUUID()

    val result = store.acquireFileLock(new Path("/testLock2"), processId)
    result must be(true)

    val processId2 = UUID.randomUUID()
    val result2 = store.acquireFileLock(new Path("/testLock2"), processId2)
    result2 must be(false)
  }

  it should "release lock on which was acquired" in {
    val processId = UUID.randomUUID()
    val lockResult = store.acquireFileLock(new Path("/testLock3"), processId)
    lockResult must be(true)

    val releaseResult = store.releaseFileLock(new Path("/testLock3"))
    releaseResult must be(true)
  }

  it should "get lock after a process has acquired and released it" in {
    val processId = UUID.randomUUID()

    val lockResult = store.acquireFileLock(new Path("/testLock4"), processId )
    lockResult must be(true)

    val releaseResult = store.releaseFileLock(new Path("/testLock4"))
    releaseResult must be(true)

    val processId2 = UUID.randomUUID()
    val lockResult2 = store.acquireFileLock(new Path("/testLock4"), processId2)
    lockResult2 must be(true)
  }

  override def afterAll() = {
    store.dropKeyspace.get
    store.disconnect()
  }

}
