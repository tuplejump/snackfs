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
package com.tuplejump.snackfs.fs.stream

import java.io.{IOException, OutputStream}
import org.apache.hadoop.fs.Path
import org.apache.cassandra.utils.UUIDGen
import java.util.UUID
import java.nio.ByteBuffer
import org.apache.hadoop.fs.permission.FsPermission
import scala.concurrent.duration._
import com.tuplejump.snackfs.fs.model._
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.twitter.logging.Logger
import scala.concurrent.Future
import com.tuplejump.snackfs.cassandra.model.GenericOpSuccess
import scala.concurrent.ExecutionContext.Implicits.global
import com.tuplejump.snackfs.util.TryHelper
import org.apache.cassandra.thrift.UnavailableException


case class FileSystemOutputStream(store: FileSystemStore, path: Path,
                                  blockSize: Long, subBlockSize: Long,
                                  atMost: FiniteDuration) extends OutputStream {

  private lazy val log = Logger.get(getClass)

  log.debug("Creating a new file: %s", path)

  private var isClosed: Boolean = false

  private var blockId: UUID = UUIDGen.getTimeUUID

  private var subBlockOffset = 0
  private var blockOffset = 0
  private var position = 0
  private var outBuffer = ByteBuffer.allocate(subBlockSize.toInt)

  private var subBlocksMeta = Vector[SubBlockMeta]()
  private var blocksMeta = Vector[BlockMeta]()

  private var isClosing = false

  private var bytesWrittenToBlock = 0

  def write(p1: Int) = {
    if (isClosed) {
      val ex = new IOException("Stream closed")
      log.error(ex, "Failed to write as stream is closed")
      throw ex
    }
    outBuffer = outBuffer.put(p1.toByte)
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
      val slice: Array[Byte] = if (offsetTemp != 0 || lengthToWrite != buf.length) buf.slice(offsetTemp, offsetTemp + lengthToWrite) else buf
      outBuffer = outBuffer.put(slice)
      lengthTemp -= lengthToWrite
      offsetTemp += lengthToWrite
      position += lengthToWrite
      if (position == subBlockSize) {
        flush()
      }
    }
  }


  private def storeSubblock(_blockId: UUID, _subBlockMeta: SubBlockMeta, _data: ByteBuffer) {
    TryHelper.handleFailure[(UUID, SubBlockMeta, ByteBuffer), GenericOpSuccess]((store.storeSubBlock _).tupled, (_blockId, _subBlockMeta, _data)).get
  }


  private def endSubBlock() = {
    if (position != 0) {
      val subBlockMeta = SubBlockMeta(UUIDGen.getTimeUUID, subBlockOffset, position)
      log.debug("storing subBlock")
      outBuffer.limit(outBuffer.position).position(0)
      val bb = ByteBuffer.allocate(outBuffer.limit)
      bb.put(outBuffer)

      bb.rewind()

      storeSubblock(blockId, subBlockMeta, bb)

      subBlockOffset += position
      bytesWrittenToBlock += position
      subBlocksMeta = subBlocksMeta :+ subBlockMeta
      position = 0
      outBuffer.clear()
    }
  }

  private def endBlock() = {
    val subBlockLengths = subBlocksMeta.map(_.length).sum
    val block = BlockMeta(blockId, blockOffset, subBlockLengths, subBlocksMeta)
    blocksMeta = blocksMeta :+ block
    blockOffset += subBlockLengths.asInstanceOf[Int]
    subBlocksMeta = Vector.empty[SubBlockMeta]
    subBlockOffset = 0
    blockId = UUIDGen.getTimeUUID
    bytesWrittenToBlock = 0
  }


  private def storeINode() {
    val user = System.getProperty("user.name")
    val permissions = FsPermission.getDefault
    val timestamp = System.currentTimeMillis()
    val iNode = INode(user, user, permissions, FileType.FILE, blocksMeta, timestamp)
    log.debug("storing/updating block details for INode at %s", path)
    TryHelper.handleFailure[(Path, INode), GenericOpSuccess]((store.storeINode _).tupled, (path, iNode))
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
      storeINode()
      super.close()
      isClosed = true
    }
  }
}
