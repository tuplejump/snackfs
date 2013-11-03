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

import org.apache.hadoop.fs.{Path, FSInputStream}
import java.io.{IOException, InputStream}
import scala.concurrent.Await
import scala.concurrent.duration._
import java.util.Date
import com.twitter.logging.Logger

case class FileSystemInputStream(store: FileSystemStore, path: Path) extends FSInputStream {

  private val log = Logger.get(getClass)

  private val INODE = Await.result(store.retrieveINode(path), 10 seconds)
  private val FILE_LENGTH: Long = INODE.blocks.map(_.length).sum

  private var currentPosition: Long = 0L

  private var blockStream: InputStream = null

  private var currentBlockSize: Long = -1

  private var currentBlockOffset: Long = 0

  private var isClosed: Boolean = false

  def seek(target: Long) = {
    if (target > FILE_LENGTH) {
      val ex = new IOException("Cannot seek after EOF")
      log.error(ex, "EOF reached earlier")
      throw ex
    }
    currentPosition = target
    currentBlockSize = -1
    currentBlockOffset = 0
  }

  def getPos: Long = currentPosition

  def seekToNewSource(targetPos: Long): Boolean = false

  private def findBlock(targetPosition: Long): InputStream = {
    val blockIndex = INODE.blocks.indexWhere(b => b.offset + b.length > targetPosition)
    if (blockIndex == -1) {
      val ex = new IOException("Impossible situation: could not find position " + targetPosition)
      log.error(ex, "Position %s could not be located", targetPosition.toString)
      throw ex
    }
    val block = INODE.blocks(blockIndex)
    currentBlockSize = block.length
    currentBlockOffset = block.offset

    val offset = targetPosition - currentBlockOffset
    log.debug("fetching block at position %s", targetPosition.toString)
    val bis = store.retrieveBlock(block)
    bis.skip(offset)
    bis
  }

  def read(): Int = {
    if (isClosed) {
      val ex = new IOException("Stream closed")
      log.error(ex, "Failed to read as stream is closed")
      throw ex
    }
    var result: Int = -1

    if (currentPosition < FILE_LENGTH) {
      if (currentPosition > currentBlockOffset + currentBlockSize) {
        if (blockStream != null) {
          blockStream.close()
        }
        log.debug("fetching next block")
        blockStream = findBlock(currentPosition)
      }
      log.debug("reading from block")
      result = blockStream.read
      if (result >= 0) {
        currentPosition += 1
      }
    }
    result
  }

  override def available: Int = (FILE_LENGTH - currentPosition).asInstanceOf[Int]

  override def read(buf: Array[Byte], off: Int, len: Int): Int = {
    if (isClosed) {
      val ex = new IOException("Stream closed")
      log.error(ex, "Failed to read as stream is closed")
      throw ex
    }
    if (buf == null) {
      val ex = new NullPointerException
      log.error(ex, "Failed to read as output buffer is null")
      throw ex
    }
    if ((off < 0) || (len < 0) || (len > buf.length - off)) {
      val ex = new IndexOutOfBoundsException
      log.error(ex, "Failed to read as one of offset,length or output buffer length is invalid")
      throw ex
    }

    var result: Int = 0
    if (len > 0) {
      while (result < len && currentPosition <= FILE_LENGTH - 1) {
        if (currentPosition > currentBlockOffset + currentBlockSize - 1) {

          if (blockStream != null) {
            blockStream.close()
          }
          log.debug("fetching next block")
          blockStream = findBlock(currentPosition)
        }
        val realLen: Int = math.min(len - result, currentBlockSize + 1).asInstanceOf[Int]
        log.debug("reading from block")
        var readSize = blockStream.read(buf, off + result, realLen)
        result += readSize
        currentPosition += readSize
      }
      if (result == 0) {
        result = -1
      }
    }
    result
  }

  override def close() = {
    if (!isClosed) {
      if (blockStream != null) {
        log.debug("closing stream")
        blockStream.close()
      }
      super.close()
      isClosed = true
    }
  }
}
