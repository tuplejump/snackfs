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
package com.tuplejump.snackfs.api.model

import scala.concurrent.Await
import scala.util.{Failure, Success, Try}
import java.io.IOException
import org.apache.hadoop.fs.permission.FsPermission
import com.tuplejump.snackfs.fs.model.{FileType, INode}
import com.tuplejump.snackfs.fs.stream.FileSystemOutputStream
import org.apache.hadoop.fs.{Path, FSDataOutputStream}
import scala.concurrent.duration.FiniteDuration
import org.apache.hadoop.util.Progressable
import java.util.UUID
import org.apache.hadoop.fs.FileSystem.Statistics
import com.twitter.logging.Logger
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.api.partial.Command

object CreateFileCommand extends Command {

  private lazy val log = Logger.get(getClass)

  def apply(store: FileSystemStore,
            filePath: Path,
            filePermission: FsPermission,
            overwrite: Boolean,
            bufferSize: Int,
            replication: Short,
            blockSize: Long,
            progress: Progressable,
            processId: UUID,
            statistics: Statistics,
            subBlockSize: Long,
            atMost: FiniteDuration): FSDataOutputStream = {

    val isCreatePossible = Await.result(store.acquireFileLock(filePath, processId), atMost)
    if (isCreatePossible) {

      try {
        val mayBeFile = Try(Await.result(store.retrieveINode(filePath), atMost))
        mayBeFile match {

          case Success(file: INode) =>
            if (file.isFile && !overwrite) {
              val ex = new IOException("File exists and cannot be overwritten")
              log.error(ex, "Failed to create file %s as it exists and cannot be overwritten", filePath)
              throw ex

            } else if (file.isDirectory) {
              val ex = new IOException("Directory with same name exists")
              log.error(ex, "Failed to create file %s as a directory with that name exists", filePath)
              throw ex
            }

          case Failure(e: Exception) =>
            val parentPath = filePath.getParent

            if (parentPath != null) {
              MakeDirectoryCommand(store, parentPath, filePermission, atMost)
            }
        }

        log.debug("creating file %s", filePath)
        val user = System.getProperty("user.name")
        val permissions = FsPermission.getDefault
        val timestamp = System.currentTimeMillis()
        val iNode = INode(user, user, permissions, FileType.FILE, List(), timestamp)
        Await.ready(store.storeINode(filePath, iNode), atMost)

        val fileStream = new FileSystemOutputStream(store, filePath, blockSize, subBlockSize, bufferSize, atMost)
        val fileDataStream = new FSDataOutputStream(fileStream, statistics)

        fileDataStream
      }

      finally {
        store.releaseFileLock(filePath)
      }
    }
    else {
      val ex = new IOException("Acquire lock failure")
      log.error(ex, "Could not get lock on file %s", filePath)
      throw ex
    }
  }

}
