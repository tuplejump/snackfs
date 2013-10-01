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

package com.tuplejump.model

import org.apache.hadoop.fs.permission.FsPermission
import java.io.{DataInputStream, InputStream, DataOutputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.util.UUID

object FileType extends Enumeration {
  val DIRECTORY, FILE = Value
}

case class INode(user: String, group: String, permission: FsPermission,
                 fileType: FileType.Value, blocks: Seq[BlockMeta], timestamp: Long) {

  def isDirectory: Boolean = this.fileType == FileType.DIRECTORY

  def isFile: Boolean = this.fileType == FileType.FILE

  def serialize: ByteBuffer = {

    // Write INode Header
    val byteStream: ByteArrayOutputStream = new ByteArrayOutputStream
    val outputStream: DataOutputStream = new DataOutputStream(byteStream)

    outputStream.writeInt(user.getBytes.length)
    outputStream.writeBytes(user)
    outputStream.writeInt(group.getBytes.length)
    outputStream.writeBytes(group)
    outputStream.writeShort(permission.toShort)
    outputStream.writeByte(fileType.id)
    if (isFile) {

      //Write Blocks
      outputStream.writeInt(blocks.length)
      blocks.foreach(b => {
        outputStream.writeLong(b.id.getMostSignificantBits)
        outputStream.writeLong(b.id.getLeastSignificantBits)
        outputStream.writeLong(b.offset)
        outputStream.writeLong(b.length)

        outputStream.writeInt(b.subBlocks.length)
        // Write SubBlocks for this block
        b.subBlocks.foreach(sb => {
          outputStream.writeLong(sb.id.getMostSignificantBits)
          outputStream.writeLong(sb.id.getLeastSignificantBits)
          outputStream.writeLong(sb.offset)
          outputStream.writeLong(sb.length)
        })
      })
    }
    outputStream.close()
    ByteBuffer.wrap(byteStream.toByteArray)
  }
}

object INode {
  def deserialize(inputStream: InputStream, timestamp: Long): INode = {
    var result: INode = null
    if (inputStream != null) {
      val dataInputStream: DataInputStream = new DataInputStream(inputStream)

      val userLength: Int = dataInputStream.readInt
      val userBuffer: Array[Byte] = new Array[Byte](userLength)
      dataInputStream.readFully(userBuffer)

      val groupLength: Int = dataInputStream.readInt
      val groupBuffer: Array[Byte] = new Array[Byte](groupLength)
      dataInputStream.readFully(groupBuffer)

      val perms: FsPermission = new FsPermission(dataInputStream.readShort)

      val fType: FileType.Value = FileType(dataInputStream.readByte)

      fType match {
        case FileType.DIRECTORY => {
          result = INode(new String(userBuffer), new String(groupBuffer), perms, fType, null, timestamp)
        }
        case FileType.FILE => {
          val blockLength = dataInputStream.readInt
          var fileBlocks: Seq[BlockMeta] = Nil
          val blockRange = 0 until blockLength
          blockRange.foreach(_ => {
            val mostSigBits: Long = dataInputStream.readLong
            val leastSigBits: Long = dataInputStream.readLong
            val offset: Long = dataInputStream.readLong
            val length: Long = dataInputStream.readLong

            // Deserialize SubBlocks for this block
            val numSubBlocks: Int = dataInputStream.readInt

            var subBlocks: Seq[SubBlockMeta] = Nil
            val subBlockRange = 0 until numSubBlocks
            subBlockRange.foreach(_ => {
              val subMostSigBits: Long = dataInputStream.readLong
              val subLeastSigBits: Long = dataInputStream.readLong
              val subOffset: Long = dataInputStream.readLong
              val subLength: Long = dataInputStream.readLong
              subBlocks = subBlocks :+ SubBlockMeta(new UUID(subMostSigBits, subLeastSigBits), subOffset, subLength)
            })
            fileBlocks = fileBlocks :+ BlockMeta(new UUID(mostSigBits, leastSigBits), offset, length, subBlocks)
          })
          result = INode(new String(userBuffer), new String(groupBuffer), perms, fType, fileBlocks, timestamp)
        }
        case _ => throw new IllegalArgumentException("Cannot deserialize INode.")
      }
    }
    result
  }
}

