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

import java.io.{IOException, InputStream}
import scala.concurrent.Await
import scala.concurrent.duration._
import com.tuplejump.model.BlockMeta
import com.twitter.logging.Logger

case class
BlockInputStream(store: FileSystemStore, blockMeta: BlockMeta, atMost: FiniteDuration) extends InputStream {
  private val log = Logger.get(getClass)

  private val LENGTH = blockMeta.length

  private var isClosed: Boolean = false
  private var inputStream: InputStream = null
  private var currentPosition: Long = 0

  private var targetSubBlockSize = 0L
  private var targetSubBlockOffset = 0L


  private def findSubBlock(targetPosition: Long): InputStream = {
    val subBlockLengthTotals = blockMeta.subBlocks.scanLeft(0L)(_ + _.length).tail
    val subBlockIndex = subBlockLengthTotals.indexWhere(p => targetPosition < p)
    if (subBlockIndex == -1) {
      val ex = new IOException("Impossible situation: could not find position " + targetPosition)
      log.error(ex, "Position %s could not be located", targetPosition.toString)
      throw ex
    }
    var offset = targetPosition
    if (subBlockIndex != 0) {
      offset -= subBlockLengthTotals(subBlockIndex - 1)
    }
    val subBlock = blockMeta.subBlocks(subBlockIndex)
    targetSubBlockSize = subBlock.length
    targetSubBlockOffset = subBlock.offset
    log.debug("fetching subBlock for block %s and position %s", blockMeta.id.toString, targetPosition.toString)
    Await.result(store.retrieveSubBlock(blockMeta.id, subBlock.id, offset), atMost)
  }

  def read: Int = {
    if (isClosed) {
      val ex = new IOException("Stream closed")
      log.error(ex,"Failed to read as stream is closed")
      throw ex
    }
    var result = -1
    if (currentPosition <= LENGTH - 1) {
      if (currentPosition > (targetSubBlockOffset + targetSubBlockSize - 1)) {
        if (inputStream != null) {
          inputStream.close()
        }
        log.debug("fetching next subblock")
        inputStream = findSubBlock(currentPosition)
      }
      log.debug("reading from subblock")
      result = inputStream.read()
      currentPosition += 1
    }
    result
  }

  override def read(buf: Array[Byte], off: Int, len: Int): Int = {
    if (isClosed) {
      val ex = new IOException("Stream closed")
      log.error(ex,"Failed to read as stream is closed")
      throw ex
    }
    if (buf == null) {
      val ex = new NullPointerException
      log.error(ex,"Failed to read as output buffer is null")
      throw ex
    }
    if ((off < 0) || (len < 0) || (len > buf.length - off)) {
      val ex = new IndexOutOfBoundsException
      log.error(ex,"Failed to read as one of offset,length or output buffer length is invalid")
      throw ex
    }
    var result = 0
    if (len > 0) {
      while (result < len && currentPosition <= LENGTH - 1) {
        if (currentPosition > (targetSubBlockOffset + targetSubBlockSize - 1)) {
          if (inputStream != null) {
            inputStream.close()
          }
          log.debug("fetching next subblock")
          inputStream = findSubBlock(currentPosition)
        }
        val remaining = len - result
        val size = math.min(remaining, targetSubBlockSize)

        log.debug("reading from subblock")
        val readSize = inputStream.read(buf, off + result, size.asInstanceOf[Int])
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
      if (inputStream != null) {
        log.debug("closing stream")
        inputStream.close()
      }
      super.close()
      isClosed = true
    }
  }
}
