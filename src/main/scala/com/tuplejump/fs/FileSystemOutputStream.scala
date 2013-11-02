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

import java.io.{IOException, OutputStream}
import org.apache.hadoop.fs.Path
import org.apache.cassandra.utils.UUIDGen
import java.util.UUID
import java.nio.ByteBuffer
import org.apache.hadoop.fs.permission.FsPermission
import scala.concurrent.Await
import scala.concurrent.duration._
import com.tuplejump.model.{SubBlockMeta, FileType, INode, BlockMeta}
import com.twitter.logging.Logger

case class FileSystemOutputStream(store: FileSystemStore, path: Path,
                                  blockSize: Long, subBlockSize: Long,
                                  bufferSize: Long, atMost: FiniteDuration) extends OutputStream {

  private val log = Logger.get(getClass)

  private var isClosed: Boolean = false

  private var blockId: UUID = UUIDGen.getTimeUUID

  private var subBlockOffset = 0
  private var blockOffset = 0
  private var position = 0
  private var outBuffer = Array.empty[Byte]

  private var subBlocksMeta = List[SubBlockMeta]()
  private var blocksMeta = List[BlockMeta]()

  private var isClosing = false

  private var bytesWrittenToBlock = 0

  def write(p1: Int) = {
    if (isClosed) {
      val ex = new IOException("Stream closed")
      log.error(ex, "Failed to write as stream is closed")
      throw ex
    }
    outBuffer = outBuffer ++ Array(p1.toByte)
    position += 1
    if (position == subBlockSize) {
      flush()
    }
  }

  override def write(buf: Array[Byte], offset: Int, length: Int) = {
    if (isClosed) {
      val ex = new IOException("Stream closed")
      log.error(ex, "Failed to write as stream is closed")
      throw ex
    }
    var lengthTemp = length
    var offsetTemp = offset
    while (lengthTemp > 0) {
      val lengthToWrite = math.min(subBlockSize - position, lengthTemp).asInstanceOf[Int]
      val slice: Array[Byte] = buf.slice(offsetTemp, offsetTemp + lengthToWrite)
      outBuffer = outBuffer ++ slice
      lengthTemp -= lengthToWrite
      offsetTemp += lengthToWrite
      position += lengthToWrite
      if (position == subBlockSize) {
        flush()
      }
    }
  }

  private def endSubBlock() = {
    if (position != 0) {
      val subBlockMeta = SubBlockMeta(UUIDGen.getTimeUUID, subBlockOffset, position)
      log.debug("storing subblock")
      Await.ready(store.storeSubBlock(blockId, subBlockMeta, ByteBuffer.wrap(outBuffer)), atMost)
      subBlockOffset += position
      bytesWrittenToBlock += position
      subBlocksMeta = subBlocksMeta :+ subBlockMeta
      position = 0
      outBuffer = Array.empty[Byte]
    }
  }

  private def endBlock() = {
    val subBlockLengths = subBlocksMeta.map(_.length).sum
    val block = BlockMeta(blockId, blockOffset, subBlockLengths, subBlocksMeta)
    blocksMeta = blocksMeta :+ block
    val user = System.getProperty("user.name")
    val permissions = FsPermission.getDefault
    val timestamp = System.currentTimeMillis()
    val iNode = INode(user, user, permissions, FileType.FILE, blocksMeta, timestamp)
    log.debug("storing/updating block details for INode at %s", path)
    Await.ready(store.storeINode(path, iNode), atMost)
    blockOffset += subBlockLengths.asInstanceOf[Int]
    subBlocksMeta = List()
    subBlockOffset = 0
    blockId = UUIDGen.getTimeUUID
    bytesWrittenToBlock = 0
  }

  override def flush() = {
    if (isClosed) {
      val ex = new IOException("Stream closed")
      log.error(ex, "Failed to write as stream is closed")
      throw ex
    }
    log.debug("flushing data at %s", position)
    endSubBlock()
    if (bytesWrittenToBlock >= blockSize || isClosing) {
      endBlock()
    }
  }

  override def close() = {
    if (!isClosed) {
      log.debug("closing stream")
      isClosing = true
      flush()
      super.close()
      isClosed = true
    }
  }
}
