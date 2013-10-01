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

import org.apache.thrift.async.TAsyncClientManager
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TNonblockingSocket
import scala.concurrent.duration._
import scala.concurrent.Await

import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.apache.cassandra.thrift.Cassandra.AsyncClient
import org.apache.cassandra.thrift.Cassandra.AsyncClient.{set_keyspace_call, system_drop_keyspace_call}
import org.apache.hadoop.fs.permission.FsPermission
import java.util.UUID
import java.net.URI
import org.apache.hadoop.fs.Path
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.cassandra.utils.ByteBufferUtil
import org.apache.commons.io.IOUtils
import org.scalatest.matchers.MustMatchers
import org.apache.cassandra.thrift.NotFoundException
import org.apache.cassandra.locator.SimpleStrategy
import com.tuplejump.util.AsyncUtil
import com.tuplejump.model._
import com.tuplejump.model.GenericOpSuccess
import com.tuplejump.model.SubBlockMeta
import com.tuplejump.model.BlockMeta

class ThriftStoreSpec extends FlatSpec with BeforeAndAfterAll with MustMatchers {

  val clientManager = new TAsyncClientManager()
  val protocolFactory = new TBinaryProtocol.Factory()
  val transport = new TNonblockingSocket("127.0.0.1", 9160)

  def client = new AsyncClient(protocolFactory, clientManager, transport)

  val store = new ThriftStore(client)

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
    val replicationStrategy = classOf[SimpleStrategy].getCanonicalName
    val ks = store.createKeyspace(store.buildSchema("STORE", 1,replicationStrategy))
    val status = Await.result(ks, 5 seconds)
    assert(status.isInstanceOf[Keyspace])
  }

  it should "set keyspace to STORE" in {
    val setKeyspaceFuture = AsyncUtil.executeAsync[set_keyspace_call](client.set_keyspace("STORE", _))
    val result = Await.result(setKeyspaceFuture, 5 seconds)
    assert(result != "")
  }

  it should "create a INode" in {
    val response = store.storeINode(path, iNode)
    val responseValue: GenericOpSuccess = Await.result(response, 10 seconds)
    assert(responseValue === GenericOpSuccess())
  }


  it should "fetch created INode" in {
    val response = store.retrieveINode(path)
    val result: INode = Await.result(response, 10 seconds)
    assert(result === iNode)
  }

  it should "fetch created subBlock" in {
    Await.ready(store.storeSubBlock(block1.id, subBlockMeta1, data), 10 seconds)
    val storeResponse = store.retrieveSubBlock(block1.id, subBlockMeta1.id, 0)
    val response = Await.result(storeResponse, 10 seconds)
    val responseString = new String(IOUtils.toByteArray(response))
    responseString must be(new String(data.array()))
  }

  it should "delete all the blocks of an Inode" in {
    val blockId = UUID.randomUUID
    val blockIdSecond = UUID.randomUUID

    val subBlock = SubBlockMeta(UUID.randomUUID, 0, 128)
    val subBlockSecond = SubBlockMeta(UUID.randomUUID, 0, 128)

    Await.result(store.storeSubBlock(blockId, subBlock, ByteBufferUtil.bytes("Random test data")), 10 seconds)
    Await.result(store.storeSubBlock(blockIdSecond, subBlockSecond, ByteBufferUtil.bytes("Random test data")), 10 seconds)

    val blockMeta = BlockMeta(blockId, 0, 0, List(subBlock))
    val blockMetaSecond = BlockMeta(blockId, 0, 0, List(subBlock))

    val subBlockData = Await.result(store.retrieveSubBlock(blockMeta.id, subBlock.id, 0), 10 seconds)
    val dataString = new String(IOUtils.toByteArray(subBlockData))
    dataString must be("Random test data")

    val iNode = INode("user", "group", FsPermission.getDefault, FileType.FILE, List(blockMeta, blockMetaSecond), timestamp)

    Await.ready(store.deleteBlocks(iNode), 10 seconds)

    val exception = intercept[NotFoundException] {
      val subBlockData = Await.result(store.retrieveSubBlock(blockMeta.id, subBlock.id, 0), 10 seconds)
    }
    assert(exception.getMessage === null)
  }

  it should "fetch all sub-paths" in {
    val path1 = new Path("/tmp")
    val iNode1 = INode("user", "group", FsPermission.getDefault, FileType.DIRECTORY, null, timestamp)
    Await.ready(store.storeINode(path1, iNode1), 10 seconds)

    val path2 = new Path("/tmp/user")
    Await.ready(store.storeINode(path2, iNode1), 10 seconds)

    val path3 = new Path("/tmp/user/file")
    Await.ready(store.storeINode(path3, iNode), 10 seconds)

    val result = Await.result(store.fetchSubPaths(path1,true), 10 seconds)
    println(result.toString())

    result.size must be(2)
  }

  it should "fetch sub-paths" in {
    val path1 = new Path("/tmp")
    val iNode1 = INode("user", "group", FsPermission.getDefault, FileType.DIRECTORY, null, timestamp)
    Await.ready(store.storeINode(path1, iNode1), 10 seconds)

    val path2 = new Path("/tmp/user")
    Await.ready(store.storeINode(path2, iNode1), 10 seconds)

    val path3 = new Path("/tmp/user/file")
    Await.ready(store.storeINode(path3, iNode), 10 seconds)

    val result = Await.result(store.fetchSubPaths(path1,false), 10 seconds)
    println(result.toString())

    result.size must be(1)
  }

  override def afterAll() = {
    Await.ready(AsyncUtil.executeAsync[system_drop_keyspace_call](client.system_drop_keyspace("STORE", _)), 10 seconds)
    clientManager.stop()
  }

}
